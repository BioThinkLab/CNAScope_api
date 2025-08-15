from rest_framework import serializers
from .models import BasicAnnotationTask, RecurrentCNATask

class BasicAnnotationTaskSerializer(serializers.ModelSerializer):
    class Meta:
        model = BasicAnnotationTask
        fields = '__all__'
        read_only_fields = ['uuid', 'status', 'create_time', 'finish_time']  # 这些字段由系统自动设置
    
    def validate_ref(self, value):
        """验证参考基因组字段"""
        valid_choices = [choice[0] for choice in BasicAnnotationTask.Ref.choices]
        if value not in valid_choices:
            raise serializers.ValidationError(f"Reference genome must be one of: {', '.join(valid_choices)}")
        return value
    
    def validate_obs_type(self, value):
        """验证观测类型字段"""
        valid_choices = [choice[0] for choice in BasicAnnotationTask.ObsType.choices]
        if value not in valid_choices:
            raise serializers.ValidationError(f"Observation type must be one of: {', '.join(valid_choices)}")
        return value
    
    def validate_window_type(self, value):
        """验证窗口类型字段"""
        valid_choices = [choice[0] for choice in BasicAnnotationTask.WindowType.choices]
        if value not in valid_choices:
            raise serializers.ValidationError(f"Window type must be one of: {', '.join(valid_choices)}")
        return value
    
    def validate_value_type(self, value):
        """验证值类型字段"""
        valid_choices = [choice[0] for choice in BasicAnnotationTask.ValueType.choices]
        if value not in valid_choices:
            raise serializers.ValidationError(f"Value type must be one of: {', '.join(valid_choices)}")
        return value
    
    def validate_k(self, value):
        """验证k值必须为正整数"""
        if value is not None and value <= 0:
            raise serializers.ValidationError("k must be a positive integer")
        return value

class RecurrentCNATaskSerializer(serializers.ModelSerializer):
    # 输入文件是必须的
    
    class Meta:
        model = RecurrentCNATask
        fields = '__all__'
        read_only_fields = ['uuid', 'status', 'create_time', 'finish_time']  # 这些字段由系统自动设置
    
    def validate_ref(self, value):
        """验证参考基因组字段"""
        valid_choices = [choice[0] for choice in RecurrentCNATask.Ref.choices]
        if value not in valid_choices:
            raise serializers.ValidationError(f"Reference genome must be one of: {', '.join(valid_choices)}")
        return value
    
    def validate_obs_type(self, value):
        """验证观测类型字段"""
        valid_choices = [choice[0] for choice in RecurrentCNATask.ObsType.choices]
        if value not in valid_choices:
            raise serializers.ValidationError(f"Observation type must be one of: {', '.join(valid_choices)}")
        return value

    def validate_value_type(self, value):
        """验证值类型字段"""
        valid_choices = [choice[0] for choice in RecurrentCNATask.ValueType.choices]
        if value not in valid_choices:
            raise serializers.ValidationError(f"Value type must be one of: {', '.join(valid_choices)}")
        return value