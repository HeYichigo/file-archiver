定期归档目录下的图片。
定期功能需要操作系统的计划任务来支持。
归档范围为最近一个月以上的文件，从月初开始到月末。

原理是按照文件日期中年月的部分进行排序，从旧到新，不处理最新的文件。

执行：

```bash
python <path to>/collection.py <source path> <target path> 
```