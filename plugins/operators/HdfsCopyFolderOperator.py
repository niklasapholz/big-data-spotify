from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException
from configparser import Error
from airflow.models.baseoperator import BaseOperator
import os
from hooks.hdfs_hook import HdfsHook

class HdfsCopyFolderOperator(BaseOperator):
  """
  Does what it says.
  """
  template_fields = ('local_folder', 'remote_folder', 'hdfs_conn_id')
  ui_color = '#fcdb03'

  @apply_defaults
  def __init__(
    self,
    local_folder: str,
    remote_folder: str,
    hdfs_conn_id: str,
    *args, **kwargs
    )->None:
    super(HdfsCopyFolderOperator, self).__init__(*args, **kwargs)
    self.local_folder = local_folder
    self.remote_folder = remote_folder
    self.hdfs_conn_id = hdfs_conn_id

  def execute(self, context):
    self.log.info("HdfsCopyFolderOperator execution started.")

    for file_name in os.listdir(self.local_folder):
      local_file = self.local_folder + file_name
      remote_file = self.remote_folder + file_name
      self.log.info("Upload file '" + local_file + "' to HDFS '" + remote_file + "'.")

      hh = HdfsHook(hdfs_conn_id=self.hdfs_conn_id)
      hh.putFile(local_file, remote_file)

    self.log.info("HdfsCopyFolderOperator done.")