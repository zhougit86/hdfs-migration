### build

mvn package


### gererateDao

mvn mybatis-generator:generate

### 拷贝/压缩任务（当前只支持Parquet）

每个单个拷贝任务会记录任务启动时间

使用hive的表名查询一个路径，将其中所有文件的modification时间与数据库中的上一次任务启动时间做比较。

所有晚于上一次任务启动时间的文件会被同步压缩到/tmp/mrzip下的同一个目录下。

文件路径与其在warehouse下的路径一致，文件后拼接任务启动时间和上一次modifiaction time。

后续使用distcp任务拷贝到腾讯的相同目录下，所有命令执行失败或者Exception都会导致返回值为1被外部程序捕获

args[1]可以用来代表是否保留tmp下的目录