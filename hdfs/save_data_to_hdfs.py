#!/usr/bin/env python3
"""
برنامج حفظ بيانات درجة الحرارة في HDFS
Real-Time Temperature Data to HDFS Storage System
"""

import os
import sys
import time
import random
import json
import subprocess
from datetime import datetime
from pathlib import Path

class HDFSDataStorage:
    """فئة لإدارة تخزين البيانات في HDFS"""
    
    def __init__(self, hdfs_path="/data"):
        self.hdfs_path = hdfs_path
        self.local_temp_file = "/tmp/temperature_data.json"
        self.data_list = []
        
    def create_hdfs_directory(self):
        """إنشاء مجلد في HDFS"""
        print(f"📁 جاري إنشاء مجلد HDFS: {self.hdfs_path}")
        
        try:
            # تحقق من وجود المجلد
            result = subprocess.run(
                ['hdfs', 'dfs', '-mkdir', '-p', self.hdfs_path],
                capture_output=True,
                text=True
            )
            
            if result.returncode == 0:
                print(f"✅ تم إنشاء المجلد: {self.hdfs_path}")
            else:
                print(f"⚠️ المجلد موجود بالفعل أو حدث خطأ")
                
        except Exception as e:
            print(f"❌ خطأ في إنشاء المجلد: {e}")
    
    def simulate_sensor_data(self, num_records=50):
        """محاكاة بيانات من مستشعر درجة الحرارة"""
        print(f"\n🌡️ محاكاة {num_records} قراءة من المستشعر...")
        
        for i in range(num_records):
            # بيانات وهمية من المستشعر
            temperature = round(random.uniform(18, 45), 2)
            humidity = round(random.uniform(30, 90), 2)
            pressure = round(random.uniform(980, 1050), 2)
            
            record = {
                'record_id': i + 1,
                'sensor_id': 'SENSOR_MAIN_001',
                'temperature_celsius': temperature,
                'humidity_percent': humidity,
                'pressure_hpa': pressure,
                'timestamp': datetime.now().isoformat(),
                'location': 'Factory_Floor_A',
                'device_status': 'Active'
            }
            
            self.data_list.append(record)
            print(f"   [{i+1}/{num_records}] 🌡️ {temperature}°C | 💧 {humidity}% | 📊 {pressure} hPa")
            
            time.sleep(0.5)  # محاكاة تأخير بين القراءات
    
    def save_to_local(self):
        """حفظ البيانات محلياً مؤقتاً"""
        print(f"\n💾 حفظ البيانات محلياً في: {self.local_temp_file}")
        
        try:
            with open(self.local_temp_file, 'w', encoding='utf-8') as f:
                json.dump(self.data_list, f, ensure_ascii=False, indent=2)
            
            file_size = os.path.getsize(self.local_temp_file)
            print(f"✅ تم الحفظ بنجاح | الحجم: {file_size} bytes")
            return True
            
        except Exception as e:
            print(f"❌ خطأ في الحفظ المحلي: {e}")
            return False
    
    def upload_to_hdfs(self):
        """رفع البيانات إلى HDFS"""
        print(f"\n☁️ رفع البيانات إلى HDFS...")
        
        hdfs_file = f"{self.hdfs_path}/temperature_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        
        try:
            result = subprocess.run(
                ['hdfs', 'dfs', '-put', '-f', self.local_temp_file, hdfs_file],
                capture_output=True,
                text=True
            )
            
            if result.returncode == 0:
                print(f"✅ تم الرفع بنجاح إلى: {hdfs_file}")
                return hdfs_file
            else:
                print(f"❌ فشل الرفع: {result.stderr}")
                return None
                
        except Exception as e:
            print(f"❌ خطأ في الرفع: {e}")
            return None
    
    def list_hdfs_files(self):
        """عرض جميع الملفات في HDFS"""
        print(f"\n📂 الملفات الموجودة في {self.hdfs_path}:")
        
        try:
            result = subprocess.run(
                ['hdfs', 'dfs', '-ls', self.hdfs_path],
                capture_output=True,
                text=True
            )
            
            if result.returncode == 0:
                print(result.stdout)
            else:
                print("❌ لم يتم العثور على ملفات")
                
        except Exception as e:
            print(f"❌ خطأ: {e}")
    
    def get_hdfs_statistics(self):
        """الحصول على إحصائيات HDFS"""
        print(f"\n📊 إحصائيات HDFS:")
        
        try:
            result = subprocess.run(
                ['hdfs', 'dfs', '-du', '-s', '-h', self.hdfs_path],
                capture_output=True,
                text=True
            )
            
            if result.returncode == 0:
                print(f"   {result.stdout}")
            else:
                print("❌ لم يتمكن من الحصول على الإحصائيات")
                
        except Exception as e:
            print(f"❌ خطأ: {e}")
    
    def run_full_pipeline(self):
        """تشغيل كامل خط معالجة البيانات"""
        print("=" * 60)
        print("🚀 بدء خط معالجة البيانات الكامل")
        print("=" * 60)
        
        # الخطوة 1: إنشاء المجلد
        self.create_hdfs_directory()
        
        # الخطوة 2: محاكاة البيانات
        self.simulate_sensor_data(num_records=50)
        
        # الخطوة 3: حفظ محلياً
        if not self.save_to_local():
            print("❌ فشل الحفظ المحلي")
            return
        
        # الخطوة 4: رفع إلى HDFS
        hdfs_file = self.upload_to_hdfs()
        
        if hdfs_file:
            # الخطوة 5: عرض الملفات
            self.list_hdfs_files()
            
            # الخطوة 6: الإحصائيات
            self.get_hdfs_statistics()
            
            print("\n" + "=" * 60)
            print("✅ اكتمل خط معالجة البيانات بنجاح!")
            print("=" * 60)
        else:
            print("❌ فشل خط معالجة البيانات")

if __name__ == '__main__':
    # إنشاء كائن التخزين
    storage = HDFSDataStorage(hdfs_path="/data/temperature")
    
    # تشغيل خط المعالجة الكامل
    storage.run_full_pipeline()