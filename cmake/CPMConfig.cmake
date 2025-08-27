# cmake/CPMConfig.cmake

# ---------------------- CPM Package Registry ----------------------
# NOTE: Order matters! Dependencies should be declared in dependency order.
# NOTE: The name in package list matters! Dependencies use "Find{name}.cmake",
# so the name has to match the normal package export name for that package



set(CPM_PACKAGE_LIST
  cppzmq
  TBB
  analysis_pipeline_core
  analysis_pipeline
  midas_receiver
  unpacker_data_products_core
  midas_event_unpacker_plugin
  DataProducts
  Unpackers
  sbfoster_midas_event_unpacker_plugin
  wfd5_pipeline_plugin
)

# ---------------------- cppzmq (header-only) ----------------------
set(cppzmq_REPO   "zeromq/cppzmq")
set(cppzmq_TAG    "v4.8.1")
set(cppzmq_TARGET "")  # No target needed; header-only
set(cppzmq_DOWNLOAD_ONLY YES)
set(cppzmq_OPTIONS
  "BUILD_EXAMPLES OFF"
  "BUILD_TESTS OFF"
)

# ---------------------- oneTBB ----------------------
set(TBB_REPO   "oneapi-src/oneTBB")
set(TBB_TAG    "v2021.12.0")
set(TBB_TARGET "TBB::tbb")
set(TBB_OPTIONS
  "TBB_TEST OFF"
  "TBB_STRICT OFF" 
  "TBB_EXAMPLES OFF"
  "TBB_FIND_PACKAGE OFF"
  "CMAKE_POSITION_INDEPENDENT_CODE ON"
)

# ---------------------- analysis_pipeline_core ----------------------
set(analysis_pipeline_core_REPO   "jaca230/analysis_pipeline_core")
set(analysis_pipeline_core_TAG    "debug")
set(analysis_pipeline_core_TARGETS
  analysis_pipeline::analysis_pipeline_core
  analysis_pipeline::spdlog_header_only
  analysis_pipeline::nlohmann_json_header_only
)
set(analysis_pipeline_core_OPTIONS
  "CMAKE_POSITION_INDEPENDENT_CODE ON"
)

# ---------------------- analysis_pipeline ----------------------
set(analysis_pipeline_REPO   "jaca230/analysis_pipeline")
set(analysis_pipeline_TAG    "debug")
set(analysis_pipeline_TARGET "analysis_pipeline::analysis_pipeline")
set(analysis_pipeline_OPTIONS
  "CMAKE_POSITION_INDEPENDENT_CODE ON"
  "BUILD_EXAMPLE_PLUGIN OFF"
)

# ---------------------- midas_receiver ----------------------
set(midas_receiver_REPO   "jaca230/midas_receiver")
set(midas_receiver_TAG    "main")
set(midas_receiver_TARGET "MidasReceiver::midas_receiver")
set(midas_receiver_OPTIONS
  "CMAKE_POSITION_INDEPENDENT_CODE ON"
)

# ---------------------- unpacker_data_products_core ----------------------
set(unpacker_data_products_core_REPO   "jaca230/unpacker_data_products_core")
set(unpacker_data_products_core_TAG    "main")
set(unpacker_data_products_core_TARGET "") # Dynamically linked
set(unpacker_data_products_coren_OPTIONS
  "CMAKE_POSITION_INDEPENDENT_CODE ON"
)

# ---------------------- default_midas_unpacking_pipeline_plugin ----------------------
set(midas_event_unpacker_plugin_REPO   "jaca230/midas_event_unpacker_plugin")
set(midas_event_unpacker_plugin_TAG    "main")
set(midas_event_unpacker_plugin_TARGET "") # Dynamically linked
set(midas_event_unpacker_plugin_OPTIONS
  "CMAKE_POSITION_INDEPENDENT_CODE ON"
)

# ---------------------- mu-data-products ----------------------
set(DataProducts_REPO "git@github.com:PIONEER-Experiment/mu-data-products.git")
set(DataProducts_TAG    "main")
set(DataProducts_TARGET "") # Dynamically linked
set(DataProducts_OPTIONS
  "CMAKE_POSITION_INDEPENDENT_CODE ON"
)

# ---------------------- mu-unpackers ----------------------
set(Unpackers_REPO   "git@github.com:PIONEER-Experiment/mu-unpackers.git")
set(Unpackers_TAG    "main")
set(Unpackers_TARGET "") # Dynamically linked
set(Unpackers_OPTIONS
  "CMAKE_POSITION_INDEPENDENT_CODE ON"
)

# ---------------------- custom_midas_unpacking_pipeline_plugin ----------------------
set(sbfoster_midas_event_unpacker_plugin_REPO   "jaca230/sbfoster_midas_event_unpacker_plugin")
set(sbfoster_midas_event_unpacker_plugin_TAG    "PSI_testbeam_august_2025")
set(sbfoster_midas_event_unpacker_plugin_TARGET "") # Dynamically linked
set(sbfoster_midas_event_unpacker_plugin_OPTIONS
  "CMAKE_POSITION_INDEPENDENT_CODE ON"
)

# ---------------------- wfd5_pipeline_plugin ----------------------
set(wfd5_pipeline_plugin_REPO   "KoljaFrahm/wfd5_pipeline_plugin")
set(wfd5_pipeline_plugin_TAG    "psi_beamtime_2025")
set(wfd5_pipeline_plugin_TARGET "") # Dynamically linked
set(wfd5_pipeline_plugin_OPTIONS
  "CMAKE_POSITION_INDEPENDENT_CODE ON"
)
