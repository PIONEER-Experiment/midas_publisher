#include "data_transmitter/DataChannelManager.h"
#include "data_transmitter/DataChannelProcessesManager.h"
#include "processors/GeneralProcessorFactory.h"
#include "processors/GeneralProcessor.h"
#include "processors/CommandProcessor.h"
#include "processors/MidasEventProcessor.h"
#include "processors/MidasOdbProcessor.h"
#include "command_management/CommandRunner.h"
#include "utilities/TypeChecker.h"
#include <algorithm>
#include <iostream>
//#include <spdlog/spdlog.h>

// Default config
const std::string DEFAULT_NAME                   = "";
const int DEFAULT_PUBLISHES_PER_BATCH            = 1;
const int DEFAULT_PUBLISHES_IGNORED_AFTER_BATCH  = 0;
const int DEFAULT_EVENTS_IN_CIRCULAR_BUFFER      = 1;
const std::string DEFAULT_ZMQ_ADDRESS            = "tcp://127.0.0.1:5555";
const int DEFAULT_PERIOD_MS                      = 1000;
const std::string DEFAULT_COMMAND_STRING         = "";
const bool DEFAULT_ENABLED_VALUE                 = true;

DataChannelManager::DataChannelManager(const nlohmann::json& channelConfig, int verbose)
    : verbose(verbose) {
    for (auto it = channelConfig.begin(); it != channelConfig.end(); ++it) {
        const std::string& channelId = it.key();
        const nlohmann::json& channelData = it.value();
        spdlog::debug("Processing {}...", channelId);
        addChannel(channelId, channelData);
    }
}

bool DataChannelManager::publish() {
    bool success = true;

    for (auto& channelPair : channels) {
        if (!channelPair.second.publish()) {
            success = false;
            //spdlog::warn("Channel {} has failed to publish. [{}:{}]",
            //             channelPair.first, __FILE__, __LINE__);
            channelPair.second.printAttributes();
        }
    }

    return success;
}

DataChannel* DataChannelManager::getChannel(const std::string& channelId) {
    auto it = channels.find(channelId);
    if (it != channels.end()) {
        return &it->second;
    }
    return nullptr;
}

const std::map<std::string, DataChannel>& DataChannelManager::getChannelMap() const {
    return channels;
}

std::vector<DataChannel> DataChannelManager::getAllChannels() const {
    std::vector<DataChannel> channelObjects;
    for (const auto& pair : channels) {
        channelObjects.push_back(pair.second);
    }
    return channelObjects;
}

void DataChannelManager::addChannel(const std::string& channelId, DataChannel dataChannel) {
    channels[channelId] = dataChannel;
}

void DataChannelManager::addChannel(const std::string& channelId, const nlohmann::json& channelConfig) {
    GeneralProcessorFactory& factory = GeneralProcessorFactory::Instance();

    bool enabled = getOrDefault(channelConfig, "enabled", DEFAULT_ENABLED_VALUE, channelId, "channel config");
    if (!enabled) return;

    std::string name = getOrDefault(channelConfig, "name", std::string(DEFAULT_NAME), channelId, "channel config");
    int publishesPerBatch = getOrDefault(channelConfig, "publishes-per-batch", DEFAULT_PUBLISHES_PER_BATCH, channelId, "channel config");
    int publishesIgnoredAfterBatch = getOrDefault(channelConfig, "publishes-ignored-after-batch", DEFAULT_PUBLISHES_IGNORED_AFTER_BATCH, channelId, "channel config");
    std::string zmq_address = getOrDefault(channelConfig, "zmq-address", std::string(DEFAULT_ZMQ_ADDRESS), channelId, "channel config");
    int eventsInCircularBuffer = getOrDefault(channelConfig, "num-events-in-circular-buffer", DEFAULT_EVENTS_IN_CIRCULAR_BUFFER, channelId, "channel config");

    DataChannel dataChannel(name, publishesPerBatch, publishesIgnoredAfterBatch, zmq_address);
    DataChannelProcessesManager processesManager(eventsInCircularBuffer + 1, verbose);
    dataChannel.setDataChannelProcessesManager(processesManager);

    if (channelConfig.contains("processors")) {
        for (const auto& processorConfig : channelConfig["processors"]) {
            GeneralProcessor* processor = nullptr;

            std::string processorType = getOrDefault(processorConfig, "processor", std::string("GeneralProcessor"), channelId, "processor config");
            processor = factory.CreateProcessor(processorType);

            if (!processor) {
                spdlog::warn("Failed to create processor '{}' in channel {} [{}:{}]",
                             processorType, channelId, __FILE__, __LINE__);
                continue;
            }

            if (TypeChecker::IsInstanceOf<MidasEventProcessor>(processor)) {
                auto* midasProcessor = dynamic_cast<MidasEventProcessor*>(processor);
                if (!midasProcessor) {
                    spdlog::warn("Failed to cast to MidasEventProcessor in channel {} [{}:{}]",
                                channelId, __FILE__, __LINE__);
                    delete processor;
                    continue;
                }

                if (!processorConfig.contains("midas_receiver_config") || !processorConfig.contains("pipeline_config")) {
                    spdlog::warn("Missing 'midas_receiver_config' or 'pipeline_config' in MidasEventProcessor config for channel {} [{}:{}]",
                                channelId, __FILE__, __LINE__);
                    delete processor;
                    continue;
                }

                const nlohmann::json& midas_receiver_config = processorConfig["midas_receiver_config"];
                const nlohmann::json& pipeline_config = processorConfig["pipeline_config"];
                const nlohmann::json& event_processor_config =
                    processorConfig.contains("midas_event_processor_config") && processorConfig["midas_event_processor_config"].is_object()
                        ? processorConfig["midas_event_processor_config"]
                        : nlohmann::json::object();

                midasProcessor->Init(midas_receiver_config, pipeline_config, event_processor_config);

                int periodMs = getOrDefault(processorConfig, "period-ms", DEFAULT_PERIOD_MS, channelId, "processor config");
                midasProcessor->setPeriod(periodMs);

                dataChannel.addProcessToManager(midasProcessor);
            }
            else if (TypeChecker::IsInstanceOf<MidasOdbProcessor>(processor)) {
                auto* odbProcessor = dynamic_cast<MidasOdbProcessor*>(processor);
                if (!odbProcessor) {
                    spdlog::warn("Failed to cast to MidasOdbProcessor in channel {} [{}:{}]",
                                channelId, __FILE__, __LINE__);
                    delete processor;
                    continue;
                }

                if (!processorConfig.contains("midas_receiver_config")) {
                    spdlog::warn("Missing 'midas_receiver_config' in MidasOdbProcessor config for channel {} [{}:{}]",
                                channelId, __FILE__, __LINE__);
                    delete processor;
                    continue;
                }

                const nlohmann::json& midas_receiver_config = processorConfig["midas_receiver_config"];
                odbProcessor->Init(midas_receiver_config);

                int periodMs = getOrDefault(processorConfig, "period-ms", DEFAULT_PERIOD_MS, channelId, "processor config");
                odbProcessor->setPeriod(periodMs);

                dataChannel.addProcessToManager(odbProcessor);
            }
            else if (TypeChecker::IsInstanceOf<CommandProcessor>(processor)) {
                auto* commandProcessor = dynamic_cast<CommandProcessor*>(processor);
                if (!commandProcessor) {
                    spdlog::warn("Failed to cast to CommandProcessor in channel {} [{}:{}]",
                                 channelId, __FILE__, __LINE__);
                    delete processor;
                    continue;
                }

                std::string commandString = getOrDefault(processorConfig, "command", std::string(DEFAULT_COMMAND_STRING), channelId, "processor config");

                CommandRunner commandRunner(commandString);
                commandProcessor->setCommandRunner(commandRunner);

                int periodMs = getOrDefault(processorConfig, "period-ms", DEFAULT_PERIOD_MS, channelId, "processor config");
                commandProcessor->setPeriod(periodMs);

                dataChannel.addProcessToManager(commandProcessor);
            }
            else {
                int periodMs = getOrDefault(processorConfig, "period-ms", DEFAULT_PERIOD_MS, channelId, "processor config");
                processor->setPeriod(periodMs);
                dataChannel.addProcessToManager(processor);
            }
        }
    }

    dataChannel.updateTickTime();
    channels[channelId] = dataChannel;
}

bool DataChannelManager::removeChannel(const std::string& channelId) {
    auto it = channels.find(channelId);
    if (it != channels.end()) {
        channels.erase(it);
        return true;
    }
    return false;
}

int DataChannelManager::getGlobalTickTime() const {
    return globalTickTime;
}

void DataChannelManager::setGlobalTickTime(int tickTime) {
    globalTickTime = tickTime;
}

void DataChannelManager::setGlobalTickTime() {
    if (!channels.empty()) {
        globalTickTime = channels.begin()->second.getTickTime();
    } else {
        globalTickTime = 0;
    }

    for (const auto& channelPair : channels) {
        globalTickTime = std::gcd(globalTickTime, channelPair.second.getTickTime());
    }
}
