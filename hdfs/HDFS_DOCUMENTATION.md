# 📋 توثيق نظام تخزين بيانات درجة الحرارة في HDFS

## 🎓 مشروع التخرج - قسم Big Data

**الطالب:** EL Rowad
**الفريق:** 3 طلاب
**التاريخ:** 28/4/2026
**الجزء المنجز:** HDFS Storage Layer

---

## 📌 ملخص المشروع

نظام متكامل لمراقبة درجة الحرارة في الوقت الفعلي وتخزينها في نظام الملفات الموزع (HDFS) للتحليل التاريخي والتنبؤ بالأعطال الصناعية.

---

## 🏗️ معمارية النظام
---

## 🔧 المكونات المستخدمة

### الأجهزة والبرامج:
- **نظام التشغيل:** Windows 10/11
- **Docker Desktop:** للـ containerization
- **Apache Hadoop 3.3.6:** لـ HDFS
- **Apache Kafka 7.4.0:** لنقل البيانات
- **Apache Zookeeper 7.4.0:** لإدارة Kafka

### الموارد المتاحة:
- **RAM:** 8GB
- **Storage:** 8GB متاح
- **CPU:** 4 cores

---

## 📁 هيكل المشروع
---

## ✅ الخطوات المنفذة

### ✅ المرحلة 1: الإعداد الأساسي
- تثبيت Docker Desktop
- إنشاء مجلد المشروع: `IoT-HDFS-Project`
- إعداد البيئة والموارد

### ✅ المرحلة 2: تكوين الخدمات
- إنشاء ملف `docker-compose.yml`
- تكوين HDFS (NameNode + DataNode)
- تكوين Kafka و Zookeeper
- تشغيل جميع الـ containers

### ✅ المرحلة 3: تخزين البيانات
- إنشاء مجلد HDFS: `/data/temperature/`
- حفظ ملف بيانات JSON في HDFS
- التحقق من استقرار التخزين

### ✅ المرحلة 4: التحقق والاختبار
- قراءة البيانات من HDFS بنجاح
- عرض الإحصائيات (حجم البيانات)
- التحقق من حالة جميع الخدمات

---

## 🚀 كيفية التشغيل

### المتطلبات:
✅ Docker Desktop مثبت
✅ 8GB RAM متاح
✅ Windows PowerShell

### خطوات البدء:

#### 1️⃣ تشغيل جميع الخدمات:
```powershell
cd IoT-HDFS-Project
docker-compose up -d
```

#### 2️⃣ التحقق من الخدمات:
```powershell
docker ps
```

يجب أن تشوف:
- kafka ✅
- zookeeper ✅
- hadoop-container ✅

#### 3️⃣ إنشاء مجلد HDFS:
```powershell
docker exec -it hadoop-container bash -c "hdfs dfs -mkdir -p /data/temperature/"
```

#### 4️⃣ حفظ البيانات:
```powershell
docker cp temp_data.json hadoop-container:/tmp/
docker exec -it hadoop-container bash -c "hdfs dfs -put /tmp/temp_data.json /data/temperature/"
```

#### 5️⃣ قراءة البيانات:
```powershell
docker exec -it hadoop-container bash -c "hdfs dfs -cat /data/temperature/temp_data.json"
```

---

## 📊 البيانات المحفوظة

### هيكل البيانات JSON:
```json
{
  "record_id": 1,
  "sensor_id": "SENSOR_001",
  "temperature": 25.5,
  "humidity": 65.2,
  "timestamp": "2026-04-27T23:08:00"
}
```

### المسار في HDFS:
### حجم البيانات:
---

## 🔍 الأوامر المهمة

### عرض الملفات في HDFS:
```bash
hdfs dfs -ls /data/temperature/
```

### قراءة محتوى ملف:
```bash
hdfs dfs -cat /data/temperature/temp_data.json
```

### حساب حجم البيانات:
```bash
hdfs dfs -du -s -h /data/temperature/
```

### حذف الملفات (إن لزم):
```bash
hdfs dfs -rm -r /data/temperature/
```

---

## 📈 الميزات المنجزة

✅ تخزين دائم للبيانات في HDFS
✅ نظام قابل للتوسع (يدعم ملايين السجلات)
✅ سهولة القراءة والوصول للبيانات
✅ دعم التحليلات التاريخية
✅ نظام موثق وجاهز للعرض

---

## 🔒 النقاط الأمان والاستقرار

✅ البيانات محفوظة بشكل آمن في HDFS
✅ دعم النسخ الاحتياطية (Replication)
✅ نظام موثوق للتعامل مع الأخطاء
✅ سهولة استرجاع البيانات في حالة الطوارئ

---

## 📝 ملاحظات مهمة

⚠️ تأكد من توفر 8GB RAM قبل التشغيل
⚠️ المجلد `/data/temperature/` ينشأ تلقائياً
⚠️ البيانات تُحفظ بصيغة JSON
⚠️ كل عملية تشغيل جديدة تنشئ ملف جديد

---

## 🛑 إيقاف النظام

لإيقاف جميع الخدمات:
```powershell
docker-compose down
```

---

## ✨ النتائج النهائية

### ما تم إنجازه:
✅ نظام تخزين HDFS متكامل
✅ 3 ملفات Python جاهزة للعمل
✅ ملف توثيق شامل
✅ بيانات محفوظة واختبارات ناجحة

### الملفات المسلمة:
✅ docker-compose.yml
✅ save_data_to_hdfs.py
✅ read_from_hdfs.py
✅ save_to_hdfs.py
✅ README.md
✅ HDFS_DOCUMENTATION.md
✅ temp_data.json

---

## 👨‍💻 معلومات المشروع

**الفريق:**
- الطالب الأول: (جزء Kafka و Spark)
- الطالب الثاني: (جزء Streaming و Processing)
- الطالب الثالث (أنت): (جزء HDFS Storage) ✅

**حالة المشروع:** 
✅ **مكتمل وجاهز للعرض**

---

## 📞 الدعم والمشاكل

في حالة وجود مشاكل:
1. تحقق من حالة Docker: `docker ps`
2. تحقق من الموارد المتاحة: RAM و Storage
3. أعد تشغيل الخدمات: `docker-compose restart`
4. افحص السجلات: `docker logs hadoop-container`

---

**آخر تحديث:** 28/4/2026
**الإصدار:** 1.0 - Final
**الحالة:** ✅ مكتمل وجاهز للتسليم