#!/usr/bin/env python3
import os
import subprocess
import json
from datetime import datetime

def read_from_hdfs():
    """قراءة البيانات المحفوظة في HDFS"""
    
    print("📂 قراءة البيانات من HDFS...")
    
    # أولاً: اعرض قائمة الملفات في HDFS
    print("\n🔍 الملفات الموجودة في HDFS:")
    os.system('hdfs dfs -ls /data/')
    
    # ثانياً: اقرأ محتوى ملف معين
    print("\n📖 محتوى البيانات:")
    os.system('hdfs dfs -cat /data/temperature_data.json')
    
    # ثالثاً: احسب الإحصائيات
    print("\n📊 الإحصائيات:")
    print("- عدد الملفات المحفوظة")
    print("- إجمالي حجم البيانات")
    print("- آخر تحديث")

def analyze_temperature_data():
    """تحليل بيانات درجة الحرارة"""
    
    print("\n🔬 تحليل البيانات...")
    
    try:
        # اقرأ الملف من HDFS
        result = subprocess.run(
            ['hdfs', 'dfs', '-cat', '/data/temperature_data.json'],
            capture_output=True,
            text=True
        )
        
        if result.returncode == 0:
            data = json.loads(result.stdout)
            
            # احسب المتوسط والحد الأقصى والأدنى
            temperatures = [item['temperature'] for item in data]
            
            print(f"✅ عدد القراءات: {len(temperatures)}")
            print(f"🌡️ متوسط درجة الحرارة: {sum(temperatures) / len(temperatures):.2f}°C")
            print(f"⬆️ الحد الأقصى: {max(temperatures):.2f}°C")
            print(f"⬇️ الحد الأدنى: {min(temperatures):.2f}°C")
        
        else:
            print("❌ خطأ في قراءة الملف")
    
    except Exception as e:
        print(f"❌ خطأ في التحليل: {e}")

if __name__ == '__main__':
    read_from_hdfs()
    analyze_temperature_data()