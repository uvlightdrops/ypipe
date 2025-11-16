# Centralized context key sets used across pipeline and logging utilities
context_keys = {
    'obj': {'fc', 'app', 'storage_broker', 'storage_cache'},
    'result': {'result'},
    'path': {'repo', 'data_path', 'data_in_path', 'data_out_path', 'project_dir', 'config_dir', 'master_config_dir'},
    'cfg': {'config_d'},
    'meta': {'app_name'},
    'fc': {'frames', 'frame_groups', 'loop_item'},
}

