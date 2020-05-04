# Read Index

Read Index 机制其实是紧盯集群当前日志序列中的一条未提交(如果有)日志, 等待该日志 apply 到状态机之后再从状态机中读取结果返回给客户端.

read index = first uncommitted index or commit index

