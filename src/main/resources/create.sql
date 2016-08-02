CREATE TABLE `file_opera_log` (
  `id` varchar(100) DEFAULT NULL,
  `app_name` varchar(100) DEFAULT NULL,
  `file_path` varchar(100) DEFAULT NULL,
  `chefile_name` varchar(100) DEFAULT NULL,
  `tar_name` varchar(100) DEFAULT NULL,
  `cutt_state` varchar(100) DEFAULT NULL,
  `carry_state` varchar(100) DEFAULT NULL,
  `create_time` varchar(100) DEFAULT NULL,
  `update_time` varchar(100) DEFAULT NULL,
  `state_remark` varchar(100) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8

CREATE TABLE `file_tar_log` (
  `id` varchar(100) DEFAULT NULL,
  `file_name` varchar(100) DEFAULT NULL,
  `state` int(11) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8

CREATE TABLE `app_pid` (
  `app_name` varchar(100) DEFAULT NULL,
  `pid` varchar(100) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8


