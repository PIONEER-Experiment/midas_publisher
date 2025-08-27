#ifndef MIDAS_EVENT_PROCESSOR_H
#define MIDAS_EVENT_PROCESSOR_H

#include "processors/GeneralProcessor.h"
#include "MidasReceiver.h"
#include "analysis_pipeline/pipeline/pipeline.h"
#include "analysis_pipeline/config/config_manager.h"
#include <chrono>
#include <nlohmann/json.hpp>
#include <unordered_set>

class MidasEventProcessor : public GeneralProcessor {
public:
    explicit MidasEventProcessor(int verbose = 0);
    ~MidasEventProcessor() override;

    void Init(const nlohmann::json& midas_receiver_config,
              const nlohmann::json& pipeline_config,
              const nlohmann::json& midas_event_processor_config);

    std::vector<std::string> getProcessedOutput() override;
    bool isReadyToProcess() const override;

private:
    std::chrono::system_clock::time_point lastProcessedTime_;

    MidasReceiver& midasReceiver_;
    std::chrono::system_clock::time_point lastEventTimestamp_;
    std::chrono::system_clock::time_point lastTransitionTimestamp_;
    bool initialized_ = false;
    size_t numEventsPerRetrieval_ = 1;
    INT lastRunNumber_ = -1;
    bool clearProductsOnNewRun_ = true;
    std::unordered_set<std::string> tagsToOmitFromClear_;

    // New members for serialization control
    size_t serializeEveryNEvents_ = 1;
    size_t eventCounter_ = 0;

    std::shared_ptr<ConfigManager> configManager_;
    std::unique_ptr<Pipeline> pipeline_;

    void handleTransitions();
    void setRunNumber(INT newRunNumber);
    INT getRunNumberFromOdb(const std::string& odbPath = "/Runinfo/Run number") const;
};

#endif // MIDAS_EVENT_PROCESSOR_H
