# Read Index

Read Index 机制其实是紧盯当前日志序列中的一条日志, 等待该日志 apply 到状态机之后再从状态机中读取结果返回给客户端.

