#!/bin/bash -l
#SBATCH --ntasks=1                     # 使用的任务数量
#SBATCH --cpus-per-task=20            # 每个任务使用的CPU核心数量
#SBATCH --mem=23G                       # 内存需求
#SBATCH --time=00:30:00                # 最大运行时间 (格式为HH:MM:SS)
#SBATCH --partition=compute            # 分区名称（根据系统调整）

# 检查参数数量
if [ $# -lt 6 ]; then
    echo "Error: Missing arguments. Usage: sbatch $0 <uuid> <input_csv> <ref> <obs_type> <window_type> <k>"
    exit 1
fi

# 获取命令行参数
uuid=$1
input_csv=$2
ref=$3
obs_type=$4
window_type=$5
k=$6

# 名称直接使用UUID
name="${uuid}"

# 设置工作目录和脚本目录
script_dir="/home/platform/workspace/CNAScope/scSVAS"  # 根据实际情况调整
output_dir="/home/platform/workspace/CNAScope/CNAScope_api/workspace/${uuid}/output"   # 根据实际情况调整

# 确保输出目录存在
mkdir -p "${output_dir}"

# 检查输入文件是否存在
if [ ! -f "${input_csv}" ]; then
    echo "Error: Input CSV file does not exist: ${input_csv}"
    
    # 写入失败状态
    finished_time=$(date +"%Y-%m-%d %H:%M:%S")
    status_file="${output_dir}/status.txt"
    echo "${finished_time}" > "${status_file}"
    echo "fail" >> "${status_file}"
    
    exit 1
fi

# 设置元数据和网络文件为默认值
meta_fn="-"
nwk_fn="-"

# 记录开始时间
start_time=$(date +"%Y-%m-%d %H:%M:%S")
echo "Starting job at ${start_time}"
echo "UUID: ${uuid}"
echo "Input file: ${input_csv}"
echo "Reference: ${ref}"
echo "Observation type: ${obs_type}"
echo "Window type: ${window_type}"
echo "K value: ${k}"
echo "Output directory: ${output_dir}"

# 执行basic_cna_anno.sh脚本
echo "Running basic_cna_anno.sh..."
bash "${script_dir}/run/basic_cna_anno.sh" \
    "${name}" "${input_csv}" "${meta_fn}" "${nwk_fn}" "${k}" \
    "${output_dir}" "${ref}" "${obs_type}" "${window_type}"

# 获取脚本执行后的退出状态码
script_exit_code=$?

# 记录完成时间和状态
finished_time=$(date +"%Y-%m-%d %H:%M:%S")
status_file="${output_dir}/status.txt"

if [ $script_exit_code -ne 0 ]; then
    status="fail"
    echo "Job failed at ${finished_time}"
else
    status="success"
    echo "Job completed successfully at ${finished_time}"
fi

# 写入状态文件
echo "${finished_time}" > "${status_file}"
echo "${status}" >> "${status_file}"

# 结束脚本，返回与basic_cna_anno.sh相同的退出状态码
exit $script_exit_code