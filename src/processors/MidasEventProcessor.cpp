#include "processors/MidasEventProcessor.h"
#include <iostream>
#include <stdexcept>
#include <chrono>
#include <spdlog/spdlog.h>
#include "analysis_pipeline/core/context/input_bundle.h"

using json = nlohmann::json;

MidasEventProcessor::MidasEventProcessor(int verbose)
    : GeneralProcessor(verbose),
      midasReceiver_(MidasReceiver::getInstance()),
      lastEventTimestamp_(std::chrono::system_clock::now()),
      lastTransitionTimestamp_(std::chrono::system_clock::now()) {}

MidasEventProcessor::~MidasEventProcessor() {
    if (initialized_) {
        midasReceiver_.stop();
    }
}

void MidasEventProcessor::Init(const json& midas_receiver_config,
                               const json& pipeline_config,
                               const json& midas_event_processor_config)
{
    if (!midas_receiver_config.is_object() || !pipeline_config.is_object() || !midas_event_processor_config.is_object()) {
        throw std::invalid_argument("[MidasEventProcessor] Init requires three JSON objects.");
    }

    MidasReceiverConfig config;
    config.host = midas_receiver_config.value("host", "");
    config.experiment = midas_receiver_config.value("experiment", "");
    config.bufferName = midas_receiver_config.value("buffer", "SYSTEM");
    config.clientName = midas_receiver_config.value("client-name", "MidasEventProcessor");
    config.eventID = midas_receiver_config.value("event-id", -1);
    config.getAllEvents = midas_receiver_config.value("get-all", true);
    config.maxBufferSize = midas_receiver_config.value("buffer-size", 1000);
    config.cmYieldTimeout = midas_receiver_config.value("yield-timeout-ms", 300);
    numEventsPerRetrieval_ = midas_receiver_config.value("num-events-per-retrieval", 1);

    config.transitionRegistrations = {
        {TR_START, 100}
    };

    if (!midasReceiver_.IsInitialized()) {
        midasReceiver_.init(config);
    }
    midasReceiver_.start();

    configManager_ = std::make_shared<ConfigManager>();
    configManager_->reset();
    configManager_->addJsonObject(pipeline_config);
    if (!configManager_->validate()) {
        throw std::runtime_error("[MidasEventProcessor] Pipeline config validation failed.");
    }

    pipeline_ = std::make_unique<Pipeline>(configManager_);
    if (!pipeline_->buildFromConfig()) {
        throw std::runtime_error("[MidasEventProcessor] Failed to build pipeline.");
    }

    if (midas_event_processor_config.is_object()) {
        clearProductsOnNewRun_ = midas_event_processor_config.value("clear-products-on-new-run", true);
        serializeEveryNEvents_ = midas_event_processor_config.value("serialize-every-n-events", 1);

        if (midas_event_processor_config.contains("tags_to_omit_from_clear") &&
            midas_event_processor_config["tags_to_omit_from_clear"].is_array()) {
            for (const auto& tag : midas_event_processor_config["tags_to_omit_from_clear"]) {
                tagsToOmitFromClear_.insert(tag.get<std::string>());
            }
        }
    }

    // Immediately set internal run number from ODB
    INT currentRun = getRunNumberFromOdb();
    if (currentRun >= 0) {
        lastRunNumber_ = currentRun;
        if (verbose > 0) {
            spdlog::debug("[MidasEventProcessor] Initial run number retrieved from ODB: {}", lastRunNumber_);
        }
    } else {
        if (verbose > 0) {
            spdlog::warn("[MidasEventProcessor] Failed to retrieve initial run number from ODB. Using -1.");
        }
        lastRunNumber_ = -1;
    }

    initialized_ = true;
}

bool MidasEventProcessor::isReadyToProcess() const {
    if (!initialized_) return false;

    auto now = std::chrono::system_clock::now();
    auto elapsedMs = std::chrono::duration_cast<std::chrono::milliseconds>(now - lastProcessedTime_).count();

    return elapsedMs >= period;
}

void MidasEventProcessor::handleTransitions() {
    auto transitions = midasReceiver_.getLatestTransitions(10, lastTransitionTimestamp_);

    for (const auto& t : transitions) {
        if (t.run_number != lastRunNumber_) {
            setRunNumber(t.run_number);
        }
    }

    if (!transitions.empty()) {
        lastTransitionTimestamp_ = transitions.back().timestamp;
    }
}

void MidasEventProcessor::setRunNumber(INT newRunNumber) {
    lastRunNumber_ = newRunNumber;

    if (verbose > 0) {
        spdlog::debug("[MidasEventProcessor] Run transition detected: run {}", newRunNumber);
    }

    if (clearProductsOnNewRun_) {
        spdlog::debug("[MidasEventProcessor] Clearing data products for new run.");
        auto& dataProductManager = pipeline_->getDataProductManager();

        if (tagsToOmitFromClear_.empty()) {
            dataProductManager.clear();
        } else {
            dataProductManager.removeExcludingTags(tagsToOmitFromClear_);
        }
    }
}

INT MidasEventProcessor::getRunNumberFromOdb(const std::string& odbPath) const {
    try {
        std::string odbJsonStr = midasReceiver_.getOdb(odbPath);
        auto odbJson = json::parse(odbJsonStr);

        if (odbJson.contains("Run number") && odbJson["Run number"].is_number()) {
            return odbJson["Run number"].get<INT>();
        } else {
            spdlog::warn("[MidasEventProcessor] ODB JSON does not contain a valid 'Run number': {}", odbJsonStr);
        }
    } catch (const std::exception& e) {
        spdlog::error("[MidasEventProcessor] Failed to parse ODB for run number: {}", e.what());
    }
    return -1;
}

std::vector<std::string> MidasEventProcessor::getProcessedOutput() {
    std::vector<std::string> out;
    if (!initialized_) return out;

    auto overallStart = std::chrono::high_resolution_clock::now();

    // 1. handleTransitions
    auto transitionsStart = std::chrono::high_resolution_clock::now();
    handleTransitions();
    auto transitionsEnd = std::chrono::high_resolution_clock::now();
    auto transitionsDuration = std::chrono::duration_cast<std::chrono::microseconds>(transitionsEnd - transitionsStart).count();
    spdlog::debug("[MidasEventProcessor] handleTransitions() took {} μs ({:.3f} ms)", transitionsDuration, transitionsDuration / 1000.0);

    // 2. getLatestEvents
    auto eventsStart = std::chrono::high_resolution_clock::now();
    auto timedEvents = midasReceiver_.getLatestEvents(numEventsPerRetrieval_, lastEventTimestamp_);
    auto eventsEnd = std::chrono::high_resolution_clock::now();
    auto eventsDuration = std::chrono::duration_cast<std::chrono::microseconds>(eventsEnd - eventsStart).count();
    spdlog::debug("[MidasEventProcessor] getLatestEvents() retrieved {} event(s) in {} μs ({:.3f} ms)",
                  timedEvents.size(), eventsDuration, eventsDuration / 1000.0);

    for (size_t i = 0; i < timedEvents.size(); ++i) {
        auto& timedEvent = timedEvents[i];

        ++eventCounter_; // NEW: increment event counter

        // 3. InputBundle construction
        auto inputBundleStart = std::chrono::high_resolution_clock::now();
        InputBundle input;
        input.set("TMEvent", timedEvent->event);
        input.set("timestamp", timedEvent->timestamp);
        input.set("run_number", lastRunNumber_);
        auto inputBundleEnd = std::chrono::high_resolution_clock::now();
        auto inputBundleDuration = std::chrono::duration_cast<std::chrono::microseconds>(inputBundleEnd - inputBundleStart).count();
        spdlog::debug("[MidasEventProcessor] Event {} InputBundle construction took {} μs ({:.3f} ms)",
                      i, inputBundleDuration, inputBundleDuration / 1000.0);

        // 4. setInputData
        auto setInputStart = std::chrono::high_resolution_clock::now();
        pipeline_->setInputData(std::move(input));
        auto setInputEnd = std::chrono::high_resolution_clock::now();
        auto setInputDuration = std::chrono::duration_cast<std::chrono::microseconds>(setInputEnd - setInputStart).count();
        spdlog::debug("[MidasEventProcessor] Event {} pipeline_->setInputData() took {} μs ({:.3f} ms)",
                      i, setInputDuration, setInputDuration / 1000.0);

        // 5. execute
        auto executeStart = std::chrono::high_resolution_clock::now();
        pipeline_->execute();
        auto executeEnd = std::chrono::high_resolution_clock::now();
        auto executeDuration = std::chrono::duration_cast<std::chrono::microseconds>(executeEnd - executeStart).count();
        spdlog::debug("[MidasEventProcessor] Event {} pipeline_->execute() took {} μs ({:.3f} ms)",
                      i, executeDuration, executeDuration / 1000.0);

        // 6. serialization (conditionally)
        json serializedData;
        if (serializeEveryNEvents_ == 0 || eventCounter_ % serializeEveryNEvents_ == 0) {
            auto serializeStart = std::chrono::high_resolution_clock::now();
            serializedData = pipeline_->getDataProductManager().serializeAll();
            auto serializeEnd = std::chrono::high_resolution_clock::now();
            auto serializeDuration = std::chrono::duration_cast<std::chrono::microseconds>(serializeEnd - serializeStart).count();
            spdlog::debug("[MidasEventProcessor] Event {} serialization took {} μs ({:.3f} ms)",
                          i, serializeDuration, serializeDuration / 1000.0);
        } else {
            spdlog::debug("[MidasEventProcessor] Event {} skipped serialization (serializeEveryNEvents = {})",
                          i, serializeEveryNEvents_);
        }

        // 7. JSON construction and dump (only if serialized)
        if (!serializedData.is_null()) {
            auto dumpStart = std::chrono::high_resolution_clock::now();
            json outJson;
            outJson["run_number"] = lastRunNumber_;
            outJson["data_products"] = serializedData;
            out.push_back(outJson.dump());
            auto dumpEnd = std::chrono::high_resolution_clock::now();
            auto dumpDuration = std::chrono::duration_cast<std::chrono::microseconds>(dumpEnd - dumpStart).count();
            spdlog::debug("[MidasEventProcessor] Event {} final JSON dump took {} μs ({:.3f} ms)",
                          i, dumpDuration, dumpDuration / 1000.0);
        }
    }

    if (!timedEvents.empty()) {
        lastEventTimestamp_ = timedEvents.back()->timestamp;
    }

    lastProcessedTime_ = std::chrono::system_clock::now();
    auto overallEnd = std::chrono::high_resolution_clock::now();
    auto overallDuration = std::chrono::duration_cast<std::chrono::microseconds>(overallEnd - overallStart).count();
    spdlog::debug("[MidasEventProcessor] Total getProcessedOutput() time for {} event(s): {} μs ({:.3f} ms)",
                  timedEvents.size(), overallDuration, overallDuration / 1000.0);

    return out;
}
