#!/usr/bin/env python3
import os
import sys
import time
import random
import json
from datetime import datetime

# اتصل بـ Kafka
from kafka import KafkaProducer

def send_temperature_to_kafka():
    """إرسال بيانات درجة الحرارة إلى Kafka"""
    
    # إعدادات Kafka
    producer = KafkaProducer(
        bootstrap_servers=['kafka:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    topic = 'temp-data'
    
    print("🌡️ بدء إرسال بيانات درجة الحرارة إلى Kafka...")
    
    try:
        for i in range(100):  # إرسال 100 قراءة
            # محاكاة بيانات درجة الحرارة من مستشعر
            temperature = round(random.uniform(20, 40), 2)
            timestamp = datetime.now().isoformat()
            
            data = {
                'sensor_id': 'SENSOR_001',
                'temperature': temperature,
                'timestamp': timestamp,
                'unit': 'Celsius'
            }
            
            # إرسال البيانات
            producer.send(topic, value=data)
            print(f"✅ تم إرسال: {data}")
            
            time.sleep(3)  # انتظر 3 ثواني قبل القراءة التالية
    
    except Exception as e:
        print(f"❌ خطأ: {e}")
    
    finally:
        producer.close()
        print("✅ تم إغلاق الاتصال")

if __name__ == '__main__':
    send_temperature_to_kafka()