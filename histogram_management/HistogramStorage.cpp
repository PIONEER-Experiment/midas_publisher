#include "HistogramStorage.h"
#include "ProjectPrinter.h"
#include "TBufferJSON.h"
#include "TString.h"
#include "TH1D.h"
#include "TH2D.h"
#include "nlohmann/json.hpp"


HistogramStorage::HistogramStorage() : needToResetHistograms(false) {
}

HistogramStorage::~HistogramStorage() {
    // Destructor implementation
    for (auto& entry : histogramMap) {
        delete entry.second; // Release memory for each histogram
    }
}

HistogramStorage& HistogramStorage::getInstance() {
    static HistogramStorage instance;
    return instance;
}

TH1* HistogramStorage::getHistogram(std::string key) {
    // Implementation to get or create a histogram
    auto it = histogramMap.find(key);
    if (it != histogramMap.end()) {
        // key exists, return a pointer to the existing histogram
        return it->second;
    } else {
        return nullptr;
    }
}

void HistogramStorage::addToHistogram(std::string key, double data) {
    if (needToResetHistograms) {
        resetHistograms();
    }
    ProjectPrinter printer;
    // Check if the key exists in the map
    auto it = histogramMap.find(key);

    if (it != histogramMap.end()) {
        // key exists, add data to the existing histogram
        it->second->Fill(data);
    } else {
        // key doesn't exist, create a new histogram and add data
        TH1D* newHistogram = new TH1D(("Histogram_" + key).c_str(), "", 100, 0, 250000);
        newHistogram->Fill(data);

        // Add the new histogram to the map
        histogramMap[key] = newHistogram;
    }
}

void HistogramStorage::addToHistogram(std::string key, double dataX, double dataY) {
    if (needToResetHistograms) {
        resetHistograms();
    }
    // Check if the key exists in the map
    auto it = histogramMap.find(key);

    if (it != histogramMap.end()) {
        // key exists, add data to the existing histogram
        it->second->Fill(dataX,dataY);
    } else {
        // key doesn't exist, create a new histogram and add data
        //TH2D(name, title, Xbins, Xmin, Xmax, Ybins, Ymin, Ymax)
        TH2D* newHistogram = new TH2D(("Histogram_" + key).c_str(), "", 12, -6, 6, 12, -6, 6);
        newHistogram->Fill(dataX,dataY);

        // Add the new histogram to the map
        histogramMap[key] = newHistogram;
    }
}

std::string HistogramStorage::serialize() {
    nlohmann::json serializedHistogramMap;

    for (const auto& entry : histogramMap) {
        const std::string& key = entry.first;
        const TH1* histogram = entry.second;

        // Serialize the histogram to a string (you might want to adjust this part based on your needs)
        TString histoTstring = TBufferJSON::ToJSON(histogram);
        std::string histoString(histoTstring.Data());

        try {
            nlohmann::json histoJson = nlohmann::json::parse(histoString);
            serializedHistogramMap[key] = histoJson;
        } catch (const nlohmann::json::exception& e) {
            ProjectPrinter printer;
            printer.PrintWarning("Error parsing histogram JSON for key " + key + ": " + std::string(e.what()), __LINE__, __FILE__);
        }
    }

    // Return the JSON string
    return serializedHistogramMap.dump();
}

void HistogramStorage::setRunNumber(int run_number) {
    if (runNumber != run_number) {
        needToResetHistograms = true;
    }
    runNumber = run_number;
}
int HistogramStorage::getRunNumber() {
    return runNumber;
}

void HistogramStorage::resetHistograms() {
    for (auto& entry : histogramMap) {
        TH1* histogram = entry.second;
        if (histogram) {
            histogram->Reset();
        }
    }
    needToResetHistograms = false;
}
