#ifndef CRPROCESSOR_H
#define CRPROCESSOR_H

#include "CommandProcessor.h"
#include "EventProcessor.h"

class CRProcessor : public CommandProcessor {
public:
    CRProcessor(const std::string& detectorMappingFile, int verbose = 0, std::shared_ptr<CommandRunner> runner = nullptr);

    std::vector<std::string> getProcessedOutput() override;

private:
    std::string detectorMappingFile;
    std::shared_ptr<EventProcessor> eventProcessor;
};

#endif // CRPROCESSOR_H