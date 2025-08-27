#include "data_transmitter/DataChannel.h"
#include "data_transmitter/DataTransmitterManager.h"
#include "data_transmitter/DataTransmitter.h"
//#include <spdlog/spdlog.h>

const int DEFAULT_CHANNEL_TICK_TIME = 1000;

// Constructors
DataChannel::DataChannel()
    : name(""), eventsBeforeBreak(1), eventsToIgnoreInBreak(0), address(""),
      eventsPublished(0), eventsSeen(0), onBreak(false), eventsSeenOnBreak(0) {
}

DataChannel::DataChannel(const std::string& name, int eventsBeforeBreak, int eventsToIgnoreInBreak)
    : name(name), eventsBeforeBreak(eventsBeforeBreak), eventsToIgnoreInBreak(eventsToIgnoreInBreak), address(""),
      eventsPublished(0), eventsSeen(0), onBreak(false), eventsSeenOnBreak(0) {
}

DataChannel::DataChannel(const std::string& name, int eventsBeforeBreak, int eventsToIgnoreInBreak, const std::string& address)
    : name(name), eventsBeforeBreak(eventsBeforeBreak), eventsToIgnoreInBreak(eventsToIgnoreInBreak), address(address),
      eventsPublished(0), eventsSeen(0), onBreak(false), eventsSeenOnBreak(0) {
    initializeTransmitter();
}

bool DataChannel::publish() {
    if (!transmitter->isBound()) {
        if (!transmitter->bind()) {
            return false;
        }
    }
    if (processesManager.runProcesses()) {
        std::string serializedData = processesManager.getDataBuffer().SerializeBuffer();
        return transmitter->publish(*this, serializedData);
    }
    return false;
}

void DataChannel::updateTickTime() {
    processesManager.updateProcessorPeriodsGCD();
    tickTime = processesManager.getProcessorPeriodsGCD();
}

void DataChannel::setName(const std::string& name) {
    this->name = name;
}

void DataChannel::setEventsBeforeBreak(int eventsBeforeBreak) {
    this->eventsBeforeBreak = eventsBeforeBreak;
}

void DataChannel::setEventsToIgnoreInBreak(int eventsToIgnoreInBreak) {
    this->eventsToIgnoreInBreak = eventsToIgnoreInBreak;
}

void DataChannel::setAddress(const std::string& address) {
    this->address = address;
    initializeTransmitter();
}

void DataChannel::setDataChannelProcessesManager(DataChannelProcessesManager manager) {
    processesManager = manager;
}

void DataChannel::addProcessToManager(GeneralProcessor* processor) {
    processesManager.addProcessor(processor);
}

int DataChannel::getTickTime() const {
    return tickTime;
}

const std::string& DataChannel::getName() const {
    return name;
}

int DataChannel::getEventsBeforeBreak() const {
    return eventsBeforeBreak;
}

int DataChannel::getEventsToIgnoreInBreak() const {
    return eventsToIgnoreInBreak;
}

int DataChannel::getEventsPublished() const {
    return eventsPublished;
}

int DataChannel::getEventsSeen() const {
    return eventsSeen;
}

const std::string& DataChannel::getAddress() const {
    return address;
}

bool DataChannel::isOnBreak() const {
    return onBreak;
}

int DataChannel::getEventsSeenOnBreak() const {
    return eventsSeenOnBreak;
}

void DataChannel::published() {
    eventsPublished++;
    if (shouldTakeBreak()) {
        startBreak();
    }
    if (onBreak) {
        eventsSeenOnBreak++;
    }
}

void DataChannel::seen() {
    eventsSeen++;
    if (onBreak) {
        eventsSeenOnBreak++;
        if (eventsSeenOnBreak >= eventsToIgnoreInBreak) {
            onBreak = false;
            eventsSeenOnBreak = 0;
        }
    }
}

bool DataChannel::shouldTakeBreak() {
    return (eventsPublished % eventsBeforeBreak == 0) && (eventsPublished > 0);
}

void DataChannel::startBreak() {
    onBreak = true;
    eventsSeenOnBreak = 0;
}

void DataChannel::reset() {
    eventsPublished = 0;
    eventsSeen = 0;
    onBreak = false;
    eventsSeenOnBreak = 0;
}

void DataChannel::printAttributes() const {
    std::string attributes;
    attributes += "Data Channel Attributes:\n";
    attributes += "Name: " + name + "\n";
    attributes += "Events Before Break: " + std::to_string(eventsBeforeBreak) + "\n";
    attributes += "Events To Ignore In Break: " + std::to_string(eventsToIgnoreInBreak) + "\n";
    attributes += "Events Published: " + std::to_string(eventsPublished) + "\n";
    attributes += "Events Seen: " + std::to_string(eventsSeen) + "\n";
    attributes += "On Break: " + std::string(onBreak ? "true" : "false") + "\n";
    if (onBreak) {
        attributes += "Events Seen On Break: " + std::to_string(eventsSeenOnBreak) + "\n";
    }
    attributes += "Address: " + address + "\n";
    attributes += "Tick Time: " + std::to_string(tickTime) + "\n";

    //spdlog::debug("{}", attributes);
}

void DataChannel::initializeTransmitter() {
    DataTransmitterManager& transmitterManager = DataTransmitterManager::Instance();
    transmitter = transmitterManager.getTransmitter(address);
}
