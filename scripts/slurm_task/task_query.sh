#!/bin/bash                                                                                                                                                                      

# 检查是否传入任务名称参数
if [ -z "$1" ]; then
    echo "Error: No job name provided. Usage: $0 <job_name>"
    exit 1
fi

# 获取任务名称
job_name=$1

# 执行 squeue 并捕获输出（排除标题行）
output=$(squeue -n "$job_name" --noheader 2>&1)

# 检查命令是否执行成功
if [ $? -ne 0 ]; then
    echo "Failed to execute squeue. Error:"
    echo "$output"
    exit 1
fi

# 检查是否有查询结果
if [ -z "$output" ]; then
    echo "empty"
else
    # 提取任务状态字段（假定是第5列）
    status=$(echo "$output" | awk '{print $5}')

    # 如果任务状态是 PD，查询前面有多少任务处于 PD 状态
    if [ "$status" == "PD" ]; then
        # 查询所有任务，按优先级排序，统计位于指定任务之前且状态为 PD 的任务数
        queue_output=$(squeue --noheader --sort=-p,-t | awk '$5 == "PD" {print $1}')
        job_id=$(echo "$output" | awk '{print $1}')  # 提取当前任务的 JobID

        # 计算前面处于 PD 状态的任务数量
        count=0
        for id in $queue_output; do
            if [ "$id" == "$job_id" ]; then
                break
            fi
            ((count++))
        done
        
        echo "$status $count"
    else
        echo "$status"
    fi
fi