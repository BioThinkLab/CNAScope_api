import csv
import io
import zipfile
import os
import tempfile
import shutil
from rest_framework.decorators import api_view, parser_classes
from rest_framework.parsers import MultiPartParser, FormParser
from rest_framework.response import Response
from rest_framework import status
from django.utils import timezone
from django.conf import settings
import uuid
from datetime import datetime
from .models import *
from .serializers import *
from .slurm_sbatch import *
from .slurm_squeue import *

@api_view(["POST"])
@parser_classes([MultiPartParser, FormParser])
def submit_basic_annotation_task(request):
    """
    提交基础注释任务的API端点
    
    接收多部分表单数据，包括任务参数和输入文件
    要求输入文件必须是CSV格式，且行数不超过1001行
    直接将文件保存到input目录下的cna.csv
    """
    try:
        # 检查是否有文件上传
        if 'input_file' not in request.FILES:
            return Response({
                "success": False,
                "msg": "Input file is required"
            }, status=status.HTTP_400_BAD_REQUEST)
        
        # 获取上传的文件
        input_file = request.FILES['input_file']
        
        # 检查文件扩展名
        file_name = input_file.name
        if not file_name.lower().endswith('.csv'):
            return Response({
                "success": False,
                "msg": "Input file must be a CSV file"
            }, status=status.HTTP_400_BAD_REQUEST)
        
        # 检查CSV文件的行数
        try:
            # 读取文件内容并计算行数
            content = input_file.read().decode('utf-8')
            input_file.seek(0)  # 重置文件指针
            
            # 使用csv模块验证格式并计算行数
            csv_reader = csv.reader(io.StringIO(content))
            row_count = sum(1 for row in csv_reader)
            
            if row_count > 1000:
                return Response({
                    "success": False,
                    "msg": f"CSV file exceeds maximum allowed rows (1000). Current row count: {row_count}"
                }, status=status.HTTP_400_BAD_REQUEST)
                
        except UnicodeDecodeError:
            return Response({
                "success": False,
                "msg": "The file is not a valid UTF-8 encoded CSV file"
            }, status=status.HTTP_400_BAD_REQUEST)
        except csv.Error:
            return Response({
                "success": False,
                "msg": "The file is not a valid CSV file"
            }, status=status.HTTP_400_BAD_REQUEST)
        
        # 创建任务 UUID
        task_uuid = uuid.uuid4()
        
        # 准备数据 - 使用前端提供的所有参数，但移除input_file
        data = request.data.copy()
        data['uuid'] = task_uuid        
        # 从数据中移除input_file，因为模型不再有此字段
        if 'input_file' in data:
            del data['input_file']
        
        # 验证和保存数据
        serializer = BasicAnnotationTaskSerializer(data=data)
        
        if serializer.is_valid():
            current_time = timezone.now()

            # 保存任务 - 现在不包含input_file字段
            task = BasicAnnotationTask.objects.create(
                uuid=task_uuid,
                user=request.data.get('user', ''),
                status=BasicAnnotationTask.Status.Pending,
                create_time=current_time,
                k=request.data.get('k', 10),
                ref=request.data.get('ref', BasicAnnotationTask.Ref.hg38),
                obs_type=request.data.get('obs_type', BasicAnnotationTask.ObsType.bulk),
                window_type=request.data.get('window_type', BasicAnnotationTask.WindowType.bin),
                value_type=request.data.get('value_type', BasicAnnotationTask.ValueType.int)
            )
            input_dir = os.path.join(settings.WORKSPACE_HOME, str(task_uuid), 'input')
            output_dir = os.path.join(settings.WORKSPACE_HOME, str(task_uuid), 'output')
            os.makedirs(input_dir, exist_ok=True)
            os.makedirs(output_dir, exist_ok=True)

            # 手动将文件保存到指定位置，固定文件名为cna.csv
            file_path = os.path.join(input_dir, 'cna.csv')
            try:
                with open(file_path, 'wb+') as destination:
                    for chunk in input_file.chunks():
                        destination.write(chunk)
            except Exception as e:
                return Response({
                    "success": False,
                    "msg": f"Failed to save input file: {str(e)}"
                }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
            # 启动异步任务处理
            
            sbatch_basic_annotation_task(task_uuid)
            
            # 返回成功信息
            return Response({
                "success": True,
                "msg": "Task submitted successfully",
                "data": {
                    "uuid": str(task_uuid),
                    "name": task.name if hasattr(task, 'name') else '',  # 检查name字段是否存在
                    "user": task.user,
                    "status": task.get_status_display(),  # 获取可读的状态名称
                    "create_time": timezone.localtime(task.create_time).strftime("%Y-%m-%d %H:%M:%S"),
                    "ref": task.ref,
                    "obs_type": task.get_obs_type_display(),  # 获取可读的观测类型
                    "window_type": task.get_window_type_display(),  # 获取可读的窗口类型
                    "value_type": task.get_value_type_display(),  # 获取可读的值类型
                    "k": task.k,
                    "input_file_name": task.get_input_file_absolute_path(),  # 固定为cna.csv
                    "row_count": row_count  # 返回CSV的行数
                }
            }, status=status.HTTP_201_CREATED)
        else:
            # 返回验证错误
            return Response({
                "success": False,
                "msg": "Validation error",
                "errors": serializer.errors
            }, status=status.HTTP_400_BAD_REQUEST)
            
    except Exception as e:
        # 记录异常并返回错误信息
        import traceback
        print(traceback.format_exc())  # 打印详细错误信息到控制台
        
        return Response({
            "success": False,
            "msg": f"Server error: {str(e)}"
        }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@api_view(["POST"])
@parser_classes([MultiPartParser, FormParser])
def submit_recurrent_cna_task(request):
    """
    提交重复CNA分析任务的API端点
    
    接收多部分表单数据，包括任务参数和输入文件
    输入文件可以是CSV（行数不超过1000行）或ZIP文件（包含不超过5个CSV，每个不超过1000行）
    """
    try:
        # 检查是否有文件上传
        if 'input_file' not in request.FILES:
            return Response({
                "success": False,
                "msg": "Input file is required"
            }, status=status.HTTP_400_BAD_REQUEST)
        
        # 获取上传的文件
        input_file = request.FILES['input_file']
        file_name = input_file.name
        file_extension = os.path.splitext(file_name)[1].lower()
        
        # 创建任务 UUID
        task_uuid = uuid.uuid4()
        
        # 准备数据目录
        input_dir = os.path.join(settings.WORKSPACE_HOME, str(task_uuid), 'input')
        output_dir = os.path.join(settings.WORKSPACE_HOME, str(task_uuid), 'output')
        os.makedirs(input_dir, exist_ok=True)
        os.makedirs(output_dir, exist_ok=True)
        
        # 存储所有有效CSV文件的绝对路径
        valid_csv_paths = []
        
        # 根据文件类型进行处理
        if file_extension == '.csv':
            # 处理CSV文件
            try:
                # 读取文件内容并计算行数
                content = input_file.read().decode('utf-8')
                input_file.seek(0)  # 重置文件指针
                
                # 使用csv模块验证格式并计算行数
                csv_reader = csv.reader(io.StringIO(content))
                row_count = sum(1 for row in csv_reader)
                
                if row_count > 1000:
                    return Response({
                        "success": False,
                        "msg": f"CSV file exceeds maximum allowed rows (1000). Current row count: {row_count}"
                    }, status=status.HTTP_400_BAD_REQUEST)
                    
                # CSV文件通过验证，保存到输入目录
                csv_path = os.path.join(input_dir, 'cna.csv')
                with open(csv_path, 'wb') as dest_file:
                    input_file.seek(0)
                    for chunk in input_file.chunks():
                        dest_file.write(chunk)
                
                # 添加到有效CSV文件列表
                valid_csv_paths.append(csv_path)
                
                # CSV文件通过验证
                file_info = {"type": "csv", "files": [{"name": file_name, "rows": row_count}]}
                
            except UnicodeDecodeError:
                return Response({
                    "success": False,
                    "msg": "The file is not a valid UTF-8 encoded CSV file"
                }, status=status.HTTP_400_BAD_REQUEST)
            except csv.Error:
                return Response({
                    "success": False,
                    "msg": "The file is not a valid CSV file"
                }, status=status.HTTP_400_BAD_REQUEST)
                
        elif file_extension == '.zip':
            # 处理ZIP文件
            try:
                # 创建临时目录用于解压文件
                with tempfile.TemporaryDirectory() as temp_dir:
                    # 保存上传的ZIP文件到临时文件
                    temp_zip_path = os.path.join(temp_dir, file_name)
                    with open(temp_zip_path, 'wb') as temp_file:
                        for chunk in input_file.chunks():
                            temp_file.write(chunk)
                    
                    # 解压ZIP文件到临时目录
                    extracted_files = []
                    with zipfile.ZipFile(temp_zip_path, 'r') as zip_ref:
                        zip_ref.extractall(temp_dir)
                        
                        # 检查每个CSV文件
                        csv_files = []
                        for root, _, files in os.walk(temp_dir):
                            for file in files:
                                if file.lower().endswith('.csv'):
                                    csv_files.append({"name": file, "path": os.path.join(root, file)})
                        
                        # 检查CSV文件数量
                        if not csv_files:
                            return Response({
                                "success": False,
                                "msg": "ZIP file does not contain any CSV files"
                            }, status=status.HTTP_400_BAD_REQUEST)
                        
                        if len(csv_files) > 5:
                            return Response({
                                "success": False,
                                "msg": f"ZIP file contains too many CSV files (maximum allowed: 5). Found: {len(csv_files)}"
                            }, status=status.HTTP_400_BAD_REQUEST)
                        
                        # 验证每个CSV文件
                        validated_csv_files = []
                        for csv_file in csv_files:
                            file_path = csv_file["path"]
                            file_name = csv_file["name"]
                            
                            # 分析CSV文件行数
                            try:
                                with open(file_path, 'r', encoding='utf-8') as f:
                                    csv_reader = csv.reader(f)
                                    row_count = sum(1 for row in csv_reader)
                                    
                                    if row_count > 1000:
                                        return Response({
                                            "success": False,
                                            "msg": f"CSV file '{file_name}' in ZIP exceeds maximum allowed rows (1000). Current row count: {row_count}"
                                        }, status=status.HTTP_400_BAD_REQUEST)
                                    
                                    validated_csv_files.append({
                                        "name": file_name, 
                                        "rows": row_count,
                                        "path": file_path
                                    })
                                    
                                    # 构建目标路径
                                    dst_path = os.path.join(input_dir, file_name)
                                    extracted_files.append((file_path, dst_path))
                            except UnicodeDecodeError:
                                return Response({
                                    "success": False,
                                    "msg": f"CSV file '{file_name}' in ZIP is not a valid UTF-8 encoded file"
                                }, status=status.HTTP_400_BAD_REQUEST)
                            except csv.Error:
                                return Response({
                                    "success": False,
                                    "msg": f"CSV file '{file_name}' in ZIP is not a valid CSV file"
                                }, status=status.HTTP_400_BAD_REQUEST)
                    
                    # 复制验证通过的文件到输入目录
                    for src, dst in extracted_files:
                        shutil.copy2(src, dst)
                        valid_csv_paths.append(dst)  # 添加到有效CSV文件列表
                    
                    # ZIP文件内容通过验证
                    file_info = {
                        "type": "zip", 
                        "file_count": len(validated_csv_files),
                        "files": [{"name": f["name"], "rows": f["rows"]} for f in validated_csv_files]
                    }
                    
                    # 复制上传的ZIP文件到输入目录
                    zip_destination = os.path.join(input_dir, file_name)
                    with open(zip_destination, 'wb') as dest_file:
                        input_file.seek(0)
                        for chunk in input_file.chunks():
                            dest_file.write(chunk)
                
            except zipfile.BadZipFile:
                return Response({
                    "success": False,
                    "msg": "The file is not a valid ZIP file"
                }, status=status.HTTP_400_BAD_REQUEST)
            except Exception as e:
                return Response({
                    "success": False,
                    "msg": f"Error processing ZIP file: {str(e)}"
                }, status=status.HTTP_400_BAD_REQUEST)
        else:
            return Response({
                "success": False,
                "msg": "Input file must be a CSV or ZIP file"
            }, status=status.HTTP_400_BAD_REQUEST)
        
        # 检查是否有有效的CSV文件
        if not valid_csv_paths:
            return Response({
                "success": False,
                "msg": "No valid CSV files were found"
            }, status=status.HTTP_400_BAD_REQUEST)
            
        # 创建逗号分隔的CSV文件路径字符串
        input_files_str = ','.join(valid_csv_paths)
        
        # 准备保存任务数据
        data = request.data.copy()
        data['uuid'] = task_uuid
        data['create_time'] = timezone.now()
        data['input_file'] = input_file
        
        # 验证和保存数据
        serializer = RecurrentCNATaskSerializer(data=data)
        
        if serializer.is_valid():
            # 保存任务
            # task = serializer.save()
            # task.save()
            task = RecurrentCNATask.objects.create(
                uuid=task_uuid,
                user=request.data.get('user', ''),
                create_time=timezone.now(),
                ref=request.data.get('ref', RecurrentCNATask.Ref.hg38),
                obs_type=request.data.get('obs_type', RecurrentCNATask.ObsType.bulk),
            )
            
            # 启动异步任务处理 (如果需要)
            sbatch_recurrent_cna_task(str(task_uuid), input_files_str)
            
            # 返回成功信息
            response_data = {
                "success": True,
                "msg": "Task submitted successfully",
                "data": {
                    "uuid": str(task_uuid),
                    "user": task.user,
                    "status": task.get_status_display(),
                    "create_time": timezone.localtime(task.create_time).strftime("%Y-%m-%d %H:%M:%S"),
                    "ref": task.ref,
                    "obs_type": task.get_obs_type_display(),
                    "input_file_name": task.get_input_file_absolute_path(),
                    "input_files": valid_csv_paths,  # 包含所有有效CSV文件的路径
                    "file_info": file_info  # 包含文件类型和CSV文件信息
                }
            }
            
            return Response(response_data, status=status.HTTP_201_CREATED)
        else:
            # 返回验证错误
            return Response({
                "success": False,
                "msg": "Validation error",
                "errors": serializer.errors
            }, status=status.HTTP_400_BAD_REQUEST)
            
    except Exception as e:
        # 记录异常并返回错误信息
        import traceback
        print(traceback.format_exc())  # 打印详细错误信息到控制台
        
        return Response({
            "success": False,
            "msg": f"Server error: {str(e)}"
        }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    
def is_valid_uuid(value):
    try:
        uuid_obj = uuid.UUID(value, version=4)  # 检查是否为有效的 UUID v4
        return str(uuid_obj) == value  # 确保格式一致
    except (ValueError, TypeError):
        return False
    
@api_view(["GET"])
def query_task(request):
    """
    查询任务状态的API端点
    
    接收GET请求，通过任务UUID查询任务的当前状态
    支持查询BasicAnnotationTask或RecurrentCNATask类型的任务
    """
    try:
        # 获取任务UUID
        task_uuid = request.query_params.get('taskUUID', '')
        
        # 验证请求参数
        if task_uuid == '':
            return Response({
                "success": False,
                "msg": "Illegal Request! Task UUID is required"
            }, status=status.HTTP_400_BAD_REQUEST)
        
        # 验证UUID格式
        if not is_valid_uuid(task_uuid):
            return Response({
                "success": False,
                "msg": "Invalid Task UUID format"
            }, status=status.HTTP_400_BAD_REQUEST)
        
        # 尝试确定任务类型并查询相应的任务记录
        task = None
        task_type = None
        
        # 首先尝试查找BasicAnnotationTask
        try:
            task = BasicAnnotationTask.objects.get(pk=task_uuid)
            task_type = "BasicAnnotationTask"
        except BasicAnnotationTask.DoesNotExist:
            # 如果不是BasicAnnotationTask，尝试查找RecurrentCNATask
            try:
                task = RecurrentCNATask.objects.get(pk=task_uuid)
                task_type = "RecurrentCNATask"
            except RecurrentCNATask.DoesNotExist:
                # 两种类型都不是，返回404
                return Response({
                    "success": False,
                    "msg": f"Task with UUID {task_uuid} not found in any task type"
                }, status=status.HTTP_404_NOT_FOUND)
        
        # 选择适当的序列化器
        if task_type == "BasicAnnotationTask":
            serializer = BasicAnnotationTaskSerializer
            success_status = BasicAnnotationTask.Status.Success
            failed_status = BasicAnnotationTask.Status.Failed
            running_status = BasicAnnotationTask.Status.Running
        else:  # RecurrentCNATask
            serializer = RecurrentCNATaskSerializer
            success_status = RecurrentCNATask.Status.Success
            failed_status = RecurrentCNATask.Status.Failed
            running_status = RecurrentCNATask.Status.Running
        
        # 如果任务已经完成或失败，直接返回任务信息
        if task.status in [success_status, failed_status]:
            return Response({
                "success": True,
                "data": serializer(task).data,
                "task_type": task_type
            }, status=status.HTTP_200_OK)
        
        # 查询Slurm中的任务状态
        task_status = squeue_by_job_name(str(task.uuid))
        
        # 处理不同的任务状态
        if task_status == 'empty':
            # 任务不在队列中，检查状态文件
            status_file_path = os.path.join(settings.WORKSPACE_HOME, str(task.uuid), 'output', 'status.txt')
            try:
                with open(status_file_path, 'r') as status_file:
                    # 解析完成时间
                    finish_time_str = status_file.readline().strip()
                    task.finish_time = timezone.make_aware(
                        datetime.strptime(finish_time_str, '%Y-%m-%d %H:%M:%S')
                    )
                    
                    # 解析状态
                    raw_status = status_file.readline().strip()
                    if raw_status == 'success':
                        task.status = success_status
                    else:
                        task.status = failed_status
                    
                    # 保存更新后的任务状态
                    task.save()
                    
                    return Response({
                        "success": True,
                        "data": serializer(task).data,
                        "task_type": task_type
                    }, status=status.HTTP_200_OK)
            except FileNotFoundError:
                return Response({
                    "success": False,
                    "msg": "Status file not found. The task may be in an inconsistent state.",
                    "task_type": task_type
                }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
            except Exception as e:
                return Response({
                    "success": False,
                    "msg": f"Error reading status file: {str(e)}",
                    "task_type": task_type
                }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        
        elif task_status == 'R':
            # 任务正在运行
            task.status = running_status
            task.save()
            
            return Response({
                "success": True,
                "data": serializer(task).data,
                "task_type": task_type
            }, status=status.HTTP_200_OK)
        
        elif task_status.startswith('PD'):
            # 任务正在队列中等待
            task_data = serializer(task).data
            # 提取队列位置
            parts = task_status.split(' ')
            if len(parts) > 1:
                queue_position = parts[1]
                task_data['position'] = queue_position
            else:
                task_data['position'] = "unknown"
            
            return Response({
                "success": True,
                "data": task_data,
                "task_type": task_type
            }, status=status.HTTP_200_OK)
        
        # 其他状态情况
        return Response({
            "success": False,
            "msg": f"Unknown task status: {task_status}",
            "task_type": task_type
        }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        
    except Exception as e:
        # 捕获所有其他异常
        import traceback
        print(traceback.format_exc())  # 打印详细错误信息到控制台
        
        return Response({
            "success": False,
            "msg": f"Server error: {str(e)}"
        }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    