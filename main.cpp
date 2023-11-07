

//Project Specific headers
#include "EventProcessor.h"
#include "MidasEvent.h"
#include "MidasBank.h"
#include "MdumpPackage.h"
#include "DataTransmitter.h"
#include "DataBuffer.h"
#include "DataChannel.h"
#include "MidasConnector.h"
#include "ODBGrabber.h"
#include "ProjectPrinter.h"
#include "EventLoopManager.h"
#include "CommandRunner.h"
#include "MdumpCommandRunner.h"
#include "SignalHandler.h"
#include "JsonManager.h"
#include "GeneralProcessorFactory.h"
#include "GeneralProcessor.h"
#include "CommandProcessor.h"
#include "CRProcessor.h"
#include "ODBProcessor.h"
#include "DataTransmitterManager.h"
#include "DataChannelManager.h"

//Special "External" Headers
#include "midas.h"
#include "midasio.h"
#include "unpackers/BasicEventUnpacker.hh"
#include "unpackers/EventUnpacker.hh"
#include "serializer/Serializer.hh"
#include "dataProducts/Waveform.hh"
#include "odbxx.h"

//Standard Libraries
#include <nlohmann/json.hpp>
#include <fstream>
#include <cstdlib>
#include <iostream>
#include <sstream>
#include <chrono>
#include <thread>
#include <termios.h>
#include <unistd.h>
#include <stdio.h>

using json = nlohmann::json;
ProjectPrinter printer; // Command line printing tools for the project

// Function to register processor classes
// New processors MUST be registered here!
void registerProcessors(nlohmann::json config) {
    int verbose = config["general-settings"]["verbose"].get<int>();
    GeneralProcessorFactory& factory = GeneralProcessorFactory::Instance();
    factory.RegisterProcessor("GeneralProcessor", std::make_shared<GeneralProcessor>(verbose));
    factory.RegisterProcessor("CommandProcessor", std::make_shared<CommandProcessor>(verbose));
    factory.RegisterProcessor("CRProcessor", std::make_shared<CRProcessor>(config["data-channels"]["mdump-channel"]["processors"][0]["detector-mapping-file"].get<std::string>(),verbose));
    factory.RegisterProcessor("ODBProcessor", std::make_shared<ODBProcessor>(verbose));
}

bool initializeMidas(MidasConnector& midasConnector, const nlohmann::json& config) {
    // Set the MidasConnector properties based on the config
    midasConnector.setEventId(config["event-id"].get<short>());
    midasConnector.setTriggerMask(config["trigger-mask"].get<short>());
    midasConnector.setSamplingType(config["sampling-type"].get<int>());
    midasConnector.setBufferSize(config["buffer-size"].get<int>());
    midasConnector.setBufferName(config["buffer-name"].get<std::string>().c_str());
    midasConnector.setBufferSize(config["buffer-size"].get<int>());
    midasConnector.setTimeout(0);
    //Broken currently, keep at 0
    //midasConnector.setTimeout(config["timeout-millis"].get<int>());
    //Broken currently
    //midasConnector.SetWatchdogParams(config["call-watchdog"].get<bool>(),static_cast<DWORD>(config["watchdog-timeout-millis"].get<int>()));

    // Call the ConnectToExperiment method
    if (!midasConnector.ConnectToExperiment()) {
        return false;
    }

    // Call the OpenEventBuffer method
    if (!midasConnector.OpenEventBuffer()) {
        return false;
    }

    // Set the buffer cache size if requested
    midasConnector.SetCacheSize(config["cache-size"].get<int>());

    // Place a request for a specific event id
    if (!midasConnector.RequestEvent()) {
        return false;
    }

    return true;
}

std::vector<MdumpCommandRunner> processMdumpCommands(const json& config, const std::string& mdumpPath) {
    std::vector<MdumpCommandRunner> mdumpCommands;

    // Extract the "mdump-commands" section
    const json& mdumpCommandsConfig = config["mdump-commands"];

    // Check if "mdump-commands" exists in the JSON
    if (!mdumpCommandsConfig.is_null() && mdumpCommandsConfig.is_object()) {
        // Iterate through the commands within "mdump-commands"
        for (auto cmdIt = mdumpCommandsConfig.begin(); cmdIt != mdumpCommandsConfig.end(); ++cmdIt) {
            const json& cmd = cmdIt.value();
            if (cmd.is_object()) {
                MdumpCommandRunner mdumpCommand(mdumpPath);

                if (cmd.contains("num-events")) {
                    int numEvents = cmd["num-events"].get<int>();
                    mdumpCommand.setEventCount(numEvents);
                }
                if (cmd.contains("bank-name")) {
                    std::string bankName = cmd["bank-name"].get<std::string>();
                    mdumpCommand.setBankName(bankName);
                }
                if (cmd.contains("event-id")) {
                    int eventId = cmd["event-id"].get<int>();
                    mdumpCommand.setEventId(eventId);
                }
                if (cmd.contains("buffer-name")) {
                    std::string bufferName = cmd["buffer-name"].get<std::string>();
                    mdumpCommand.setBufferName(bufferName);
                }
                if (cmd.contains("trigger-mask")) {
                    int triggerMask = cmd["trigger-mask"].get<int>();
                    mdumpCommand.setTriggerMask(triggerMask);
                }
                if (cmd.contains("minimum-time-between-commands-millis")) {
                    int waitTime = cmd["minimum-time-between-commands-millis"].get<int>();
                    mdumpCommand.setWaitTime(waitTime);
                }

                // Add the parsed command to the vector
                mdumpCommands.push_back(mdumpCommand);
            }
        }
    }

    return mdumpCommands;
}

int findSmallestWaitTime(const std::vector<MdumpCommandRunner>& mdumpCommands) {
    int smallestWaitTime = std::numeric_limits<int>::max(); // Initialize with a large value

    for (const MdumpCommandRunner& command : mdumpCommands) {
        int waitTime = command.getWaitTime();
        if (waitTime < smallestWaitTime) {
            smallestWaitTime = waitTime;
        }
    }

    return smallestWaitTime;
}

int mdumpOff(nlohmann::json config) {
    // Save the terminal settings to restore them later
    struct termios original_termios;
    tcgetattr(STDIN_FILENO, &original_termios);

    // Set terminal to non-canonical mode with no echo
    struct termios new_termios = original_termios;
    new_termios.c_lflag &= ~(ICANON | ECHO);
    tcsetattr(STDIN_FILENO, TCSANOW, &new_termios);

    // Read the maximum event size from the JSON configuration
    INT max_event_size = config["max-event-size"].get<int>();

    // Allocate memory for storing event data dynamically
    void* event_data = malloc(max_event_size);

    // Initialize EventProcessor with detector mapping file and verbosity flag
    EventProcessor eventProcessor(config["detector-mapping-file"].get<std::string>(), config["verbose"].get<int>());

    // Initialize DataTransmitter with the ZeroMQ address
    DataTransmitter dataPublisher(config["zmq-address"].get<std::string>(), config["verbose"].get<int>());

    // Initialize DataBuffer with a specified buffer size
    DataBuffer<std::string> eventBuffer(config["num-events-in-circular-buffer"].get<size_t>() + 1);

    //Initialize EventLoopManager with specified events per sleep time and sleep duration
    // Variable inputs are broken currently
    //EventLoopManager eventLoopManager(config["events-before-sleep"].get<int>(),config["sleep-time-millis"].get<int>(),config["timeout-millis"].get<int>(),config["verbose"].get<int>());
    EventLoopManager eventLoopManager(0,0,0,config["verbose"].get<int>());

    ODBGrabber odbGrabber(config["ODB-grabber-client-name"].get<std::string>().c_str(), config["grab-ODB-interval-millis"].get<int>());

    // Make Data Channels
    DataChannel dataBankChannel(
        config["zmq-data-channel-name"].get<std::string>(),
        config["zmq-data-channel-publishes-per-batch"].get<int>(),
        config["zmq-data-channel-publishes-ignored-after-batch"].get<int>()
    );
    DataChannel odbChannel(
        config["zmq-odb-channel-name"].get<std::string>(),
        config["zmq-odb-channel-publishes-per-batch"].get<int>(),
        config["zmq-odb-channel-publishes-ignored-after-batch"].get<int>()
    );
    
    if (config["verbose"].get<int>() > 0) {
        dataBankChannel.printAttributes();
        odbChannel.printAttributes();
    }

    // Connect to the ZeroMQ server
    if (!dataPublisher.bind()) {
        // Handle connection error
        printer.PrintError("Failed to bind to port " + config["zmq-address"].get<std::string>(), __LINE__, __FILE__);
        return 1;
    } else {
        printer.Print("Connected to the ZeroMQ server.");
    }

    // Event processing loop
    bool quitRequested = false;
    char userInput;
    while (!quitRequested) {
        userInput = 0;
        if (read(STDIN_FILENO, &userInput, 1) == 1) {
            // Check if the user pressed 'q' (case-insensitive)
            if (tolower(userInput) == 'q') {
                quitRequested = true;
            }
        }

        // Initialize MidasConnector and connect to the MIDAS experiment
        MidasConnector midasConnector(config["client-name"].get<std::string>().c_str());
        if (!initializeMidas(midasConnector, config)) {
            printer.PrintError("Failed to initialize MIDAS.", __LINE__, __FILE__);
            return 1;
        }

        int success = midasConnector.ReceiveEvent(event_data,max_event_size);
        if (success == BM_SUCCESS) {
            // Process data once we have it
            eventProcessor.processEvent(event_data, max_event_size);

            // Serialize the event data with EventProcessor and store it in serializedData
            std::string serializedData = eventProcessor.getSerializedData();

            // Add serialized data to the buffer
            eventBuffer.Push(serializedData);
            std::string bufferData = eventBuffer.SerializeBuffer();

            // Send the serialized data to the ZeroMQ server with DataTransmitter
            if (!dataPublisher.publish(dataBankChannel, bufferData)) {
                printer.PrintError("Failed to send serialized data to channel: " + config["zmq-data-channel-name"].get<std::string>(), __LINE__, __FILE__);
            }
        }
        if (odbGrabber.isReadyToGrab() && config["publish-odb"].get<bool>()) {
            odbGrabber.grabODB();
            std::string odbJson = odbGrabber.getODBJson();
            if (!dataPublisher.publish(odbChannel, odbJson)) {
                printer.PrintError("Failed to send serialized data to channel: " + config["zmq-odb-channel-name"].get<std::string>(), __LINE__, __FILE__);
            }
        }

        eventLoopManager.ManageLoop(success);
        midasConnector.DisconnectFromExperiment(); // Disconnect from the MIDAS experiment
    }

    // Restore the original terminal settings
    tcsetattr(STDIN_FILENO, TCSANOW, &original_termios);
    

    return 0;
}

int mdumpOn(nlohmann::json config) {
    // Get the value of the MIDASSYS environment variable
    char* midasSysPath = std::getenv("MIDASSYS");

    if (midasSysPath == nullptr) {
        std::cerr << "MIDASSYS environment variable is not set. Please set it and try again." << std::endl;
        return 1;
    }

    std::vector<MdumpCommandRunner> mdumpCommands = processMdumpCommands(config, std::string(midasSysPath) + "/bin/mdump");
    int tickTime = findSmallestWaitTime(mdumpCommands);

    ODBGrabber odbGrabber(config["ODB-grabber-client-name"].get<std::string>().c_str(), config["grab-ODB-interval-millis"].get<int>());

    EventProcessor eventProcessor(config["detector-mapping-file"].get<std::string>(), config["verbose"].get<int>());

    // Initialize DataBuffer with a specified buffer size
    DataBuffer<std::string> eventBuffer(config["num-events-in-circular-buffer"].get<size_t>() + 1);

    // Initialize DataTransmitter with the ZeroMQ address
    DataTransmitter dataPublisher(config["zmq-address"].get<std::string>(), config["verbose"].get<int>());

    // Make Data Channels
    DataChannel dataBankChannel(
        config["zmq-data-channel-name"].get<std::string>(),
        config["zmq-data-channel-publishes-per-batch"].get<int>(),
        config["zmq-data-channel-publishes-ignored-after-batch"].get<int>()
    );
    DataChannel odbChannel(
        config["zmq-odb-channel-name"].get<std::string>(),
        config["zmq-odb-channel-publishes-per-batch"].get<int>(),
        config["zmq-odb-channel-publishes-ignored-after-batch"].get<int>()
    );

    // Connect to the ZeroMQ server
    if (!dataPublisher.bind()) {
        // Handle connection error
        printer.PrintError("Failed to bind to port " + config["zmq-address"].get<std::string>(), __LINE__, __FILE__);
        return 1;
    } else {
        printer.Print("Connected to the ZeroMQ server.");
    }

    while (!SignalHandler::getInstance().isQuitSignalReceived()) {
        for (MdumpCommandRunner& command : mdumpCommands) {
            if (!command.isReadyForExecution()) {
                continue;
            }
            if (config["verbose"].get<int>() > 0) {
                printer.Print(command.getCommand());
            }
            std::string output;
            try {
                output = command.execute();
            } catch (const std::exception& e) {
                printer.PrintError("Error: " + std::string(e.what()), __LINE__, __FILE__);
                return 1;
            }

            // Now, pass the remaining string to the MidasEvent constructor
            MdumpPackage mdumpPackage(output);
            for (const MidasEvent& event : mdumpPackage.getEvents()) {
                if (eventProcessor.processEvent(event, "CR00") == 0) {
                    // Serialize the event data with EventProcessor and store it in serializedData
                    std::string serializedData = eventProcessor.getSerializedData();
                    eventBuffer.Push(serializedData);
                }
            }

            std::string bufferData = eventBuffer.SerializeBuffer();
            // Send the serialized data to the ZeroMQ server with DataTransmitter
            if (!dataPublisher.publish(dataBankChannel, bufferData)) {
                printer.PrintError("Failed to send serialized data to channel: " + config["zmq-data-channel-name"].get<std::string>(), __LINE__, __FILE__);
            }

            if (odbGrabber.isReadyToGrab() and config["publish-odb"].get<bool>()) {
                odbGrabber.grabODB();
                std::string odbJson = odbGrabber.getODBJson();
                if (!dataPublisher.publish(odbChannel, odbJson)) {
                    printer.PrintError("Failed to send serialized data to channel: " + config["zmq-odb-channel-name"].get<std::string>(), __LINE__, __FILE__);
                }
            }

            // Sleep for the specified interval before running the next command
            std::this_thread::sleep_for(std::chrono::milliseconds(tickTime));
        }
    }

    printer.Print("Received quit signal. Exiting the loop.");

    return 0;
}

int newMode() {
    nlohmann::json config = JsonManager::getInstance().getConfig(); //Get cleaned up config
    printer.Print("Got here 1");
    DataTransmitterManager::Instance(config["general-settings"]["verbose"].get<int>()); //Initialize the DataTransmitterManager
    printer.Print("Got here 2");
    registerProcessors(config); //Register processors so we can map strings to processor objects
    printer.Print("Got here 3");
    DataChannelManager dataChannelManager(config["data-channels"],config["general-settings"]["verbose"].get<int>());
    printer.Print("Got here 4");
    dataChannelManager.setGlobalTickTime();
    int tickTime = dataChannelManager.getGlobalTickTime();
    while (!SignalHandler::getInstance().isQuitSignalReceived()) {
        printer.Print("Got in Loop 1");
        dataChannelManager.publish();
        printer.Print("Finished loop, sleeping for " + std::to_string(tickTime) + "ms ...");
        std::this_thread::sleep_for(std::chrono::milliseconds(tickTime));     
    }
    printer.Print("Got to end");
    return 0;
}

int main(int argc, char* argv[]) {
    return newMode();
    /*
    // Access the JsonManager instance and config
    nlohmann::json config = JsonManager::getInstance().getConfig();

    if (config["use-mdump"].get<bool>()) {
        return mdumpOn(config);
    } else {
        return mdumpOff(config);
    }
    */
}