import pyodbc
import pandas as pd
from pathlib import Path
from .config import CLEAN_OUTPUT, DEDUP_OUTPUT

# إعدادات SQL Server
SERVER = r'localhost\SQLEXPRESS'
DATABASE = 'UTL_DB'
DRIVER = 'ODBC Driver 17 for SQL Server'

# جداول SQL
TABLE_CLEAN = 'customers_clean'
TABLE_DEDUP = 'customers_dedup'


def create_database():
    """إنشاء قاعدة البيانات إن لم تكن موجودة"""
    try:
        # الاتصال بـ master database أولاً
        conn_str = f'Driver={DRIVER};Server={SERVER};Database=master;Trusted_Connection=yes;'
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()

        # التحقق من وجود قاعدة البيانات
        cursor.execute(f"SELECT name FROM sys.databases WHERE name = '{DATABASE}'")
        if not cursor.fetchone():
            print(f"📁 إنشاء قاعدة البيانات: {DATABASE}...")
            cursor.execute(f"CREATE DATABASE {DATABASE}")
            conn.commit()
            print(f"✅ تم إنشاء قاعدة البيانات: {DATABASE}")
        else:
            print(f"✅ قاعدة البيانات موجودة بالفعل: {DATABASE}")

        conn.close()
        return True
    except Exception as e:
        print(f"❌ خطأ في إنشاء قاعدة البيانات: {e}")
        return False


def create_tables(conn):
    """إنشاء الجداول"""
    cursor = conn.cursor()

    # جدول البيانات المنظفة
    create_clean_table = f"""
    IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = '{TABLE_CLEAN}')
    CREATE TABLE {TABLE_CLEAN} (
        customer_id INT,
        name NVARCHAR(255),
        email NVARCHAR(255),
        phone NVARCHAR(20),
        city NVARCHAR(100),
        country NVARCHAR(100),
        signup_date DATE,
        amount DECIMAL(10, 2)
    )
    """

    # جدول البيانات النهائية (مع الشذوذ)
    create_dedup_table = f"""
    IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = '{TABLE_DEDUP}')
    CREATE TABLE {TABLE_DEDUP} (
        customer_id INT,
        name NVARCHAR(255),
        email NVARCHAR(255),
        phone NVARCHAR(20),
        city NVARCHAR(100),
        country NVARCHAR(100),
        signup_date DATE,
        amount DECIMAL(10, 2),
        is_anomaly INT,
        anomaly_score FLOAT
    )
    """

    try:
        cursor.execute(create_clean_table)
        print(f"✅ تم إنشاء الجدول: {TABLE_CLEAN}")

        cursor.execute(create_dedup_table)
        print(f"✅ تم إنشاء الجدول: {TABLE_DEDUP}")

        conn.commit()
        return True
    except Exception as e:
        print(f"❌ خطأ في إنشاء الجداول: {e}")
        return False


def load_data_to_db(csv_path, table_name):
    """تحميل البيانات من CSV إلى قاعدة البيانات"""
    try:
        # قراءة البيانات
        df = pd.read_csv(csv_path)

        # الاتصال بـ SQL Server
        conn_str = f'Driver={DRIVER};Server={SERVER};Database={DATABASE};Trusted_Connection=yes;'
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()

        # حذف البيانات القديمة (اختياري)
        cursor.execute(f"TRUNCATE TABLE {table_name}")

        # إدراج البيانات
        for index, row in df.iterrows():
            # استبدال NaN بـ None
            values = [None if pd.isna(val) else val for val in row]

            # إنشاء استعلام INSERT
            placeholders = ','.join(['?' for _ in values])
            columns = ','.join(df.columns)
            query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"

            cursor.execute(query, values)

        conn.commit()
        print(f"✅ تم تحميل {len(df)} صف إلى جدول {table_name}")
        conn.close()
        return True

    except Exception as e:
        print(f"❌ خطأ في تحميل البيانات: {e}")
        return False


def verify_data():
    """التحقق من البيانات المحملة"""
    try:
        conn_str = f'Driver={DRIVER};Server={SERVER};Database={DATABASE};Trusted_Connection=yes;'
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()

        # عد الصفوف في كل جدول
        cursor.execute(f"SELECT COUNT(*) FROM {TABLE_CLEAN}")
        clean_count = cursor.fetchone()[0]

        cursor.execute(f"SELECT COUNT(*) FROM {TABLE_DEDUP}")
        dedup_count = cursor.fetchone()[0]

        cursor.execute(f"SELECT COUNT(*) FROM {TABLE_DEDUP} WHERE is_anomaly = 1")
        anomaly_count = cursor.fetchone()[0]

        conn.close()

        print("\n" + "="*50)
        print("📊 التحقق من البيانات:")
        print("="*50)
        print(f"📁 جدول {TABLE_CLEAN}: {clean_count} صف")
        print(f"📁 جدول {TABLE_DEDUP}: {dedup_count} صف")
        print(f"⚠️  صفوف شذوذ: {anomaly_count}")
        print("="*50)

        return True
    except Exception as e:
        print(f"❌ خطأ في التحقق: {e}")
        return False


def main():
    """الدالة الرئيسية"""
    print("\n" + "="*50)
    print("🗄️  تحميل البيانات إلى SQL Server")
    print("="*50)

    # الخطوة 1: إنشاء قاعدة البيانات
    print("\n[1/5] إنشاء قاعدة البيانات...")
    if not create_database():
        print("❌ فشل إنشاء قاعدة البيانات")
        return

    # الخطوة 2: الاتصال بـ SQL Server
    print("\n[2/5] الاتصال بـ SQL Server...")
    try:
        conn_str = f'Driver={DRIVER};Server={SERVER};Database={DATABASE};Trusted_Connection=yes;'
        conn = pyodbc.connect(conn_str)
        print(f"✅ تم الاتصال بـ {SERVER}\\{DATABASE}")
    except Exception as e:
        print(f"❌ فشل الاتصال: {e}")
        return

    # الخطوة 3: إنشاء الجداول
    print("\n[3/5] إنشاء الجداول...")
    if not create_tables(conn):
        print("❌ فشل إنشاء الجداول")
        conn.close()
        return

    conn.close()

    # الخطوة 4: تحميل البيانات المنظفة
    print("\n[4/5] تحميل البيانات المنظفة...")
    if CLEAN_OUTPUT.exists():
        load_data_to_db(CLEAN_OUTPUT, TABLE_CLEAN)
    else:
        print(f"⚠️ لم يتم العثور على ملف: {CLEAN_OUTPUT}")

    # الخطوة 5: تحميل البيانات النهائية
    print("\n[5/5] تحميل البيانات النهائية...")
    if DEDUP_OUTPUT.exists():
        load_data_to_db(DEDUP_OUTPUT, TABLE_DEDUP)
    else:
        print(f"⚠️ لم يتم العثور على ملف: {DEDUP_OUTPUT}")

    # التحقق من البيانات
    print("\n[التحقق] التحقق من البيانات...")
    verify_data()

    print("\n✅ تم حفظ جميع البيانات في SQL Server بنجاح!\n")
    print("🎯 للتحقق من البيانات في SSMS:")
    print(f"   SELECT * FROM {TABLE_CLEAN}")
    print(f"   SELECT * FROM {TABLE_DEDUP}")
    print(f"   SELECT * FROM {TABLE_DEDUP} WHERE is_anomaly = 1\n")


if __name__ == "__main__":
    main()
