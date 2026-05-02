import argparse
from .ingest import load_dataset
from .clean_rules import apply_cleaning_rules
from .dedup_ml import load_dedup_model, predict_duplicate_pairs, drop_predicted_duplicates
from .anomaly import load_anomaly_model, predict_anomalies
from .utils import save_json, now_iso
from .config import (
    CLEAN_OUTPUT,
    DEDUP_OUTPUT,
    QUALITY_REPORT_PATH,
    DEDUP_MODEL_PATH,
    ANOMALY_MODEL_PATH
)


def main(input_path: str):
    print(f"\n🚀 معالجة الملف: {input_path}\n")
    
    # الخطوة 1: تحميل البيانات
    print("[1/5] تحميل البيانات...")
    raw, detected_cols = load_dataset(input_path)

    # الخطوة 2: تنظيف البيانات
    print("[2/5] تنظيف البيانات...")
    clean_df, clean_report = apply_cleaning_rules(raw, detected_cols)
    clean_df.to_csv(CLEAN_OUTPUT, index=False)
    print(f"  ✅ تم تنظيف {len(clean_df)} صف")

    # الخطوة 3: الكشف عن التكرارات
    print("[3/5] الكشف عن التكرارات...")
    dedup_report = {"ml_duplicates_found": 0, "dropped_indices": []}
    try:
        dedup_model = load_dedup_model(DEDUP_MODEL_PATH)
        if dedup_model:
            dup_pairs = predict_duplicate_pairs(clean_df, dedup_model, threshold=0.75)
            dedup_df, dropped = drop_predicted_duplicates(clean_df, dup_pairs)
            dedup_report["ml_duplicates_found"] = len(dup_pairs)
            dedup_report["dropped_indices"] = dropped
            print(f"  ✅ تم حذف {len(dup_pairs)} صفوف مكررة")
        else:
            dedup_df = clean_df.copy()
            print("  ⚠️ لم يتم العثور على نموذج التكرارات")
    except Exception as e:
        dedup_df = clean_df.copy()
        print(f"  ⚠️ خطأ: {str(e)}")

    # الخطوة 4: كشف الشذوذ
    print("[4/5] كشف الشذوذ...")
    anomaly_report = {"anomalies_found": 0}
    try:
        anomaly_model = load_anomaly_model(ANOMALY_MODEL_PATH)
        if anomaly_model:
            scored_df = predict_anomalies(dedup_df, anomaly_model)
            anomaly_report["anomalies_found"] = int(scored_df["is_anomaly"].sum())
            output_df = scored_df
            print(f"  ✅ تم اكتشاف {anomaly_report['anomalies_found']} شذوذ")
        else:
            output_df = dedup_df
            print("  ⚠️ لم يتم العثور على نموذج الشذوذ")
    except Exception as e:
        output_df = dedup_df
        print(f"  ⚠️ خطأ: {str(e)}")

    # الخطوة 5: حفظ النتائج
    print("[5/5] حفظ النتائج...")
    output_df.to_csv(DEDUP_OUTPUT, index=False)

    final_report = {
        "generated_at": now_iso(),
        "input_path": input_path,
        "outputs": {
            "clean_csv": str(CLEAN_OUTPUT),
            "dedup_csv": str(DEDUP_OUTPUT),
        },
        "cleaning": clean_report,
        "deduplication": dedup_report,
        "anomaly": anomaly_report,
        "final_rows": int(len(output_df)),
    }

    save_json(QUALITY_REPORT_PATH, final_report)
    
    print("\n" + "="*50)
    print("✅ المعالجة اكتملت بنجاح!")
    print("="*50)
    print(f"📊 الملف الأصلي: {len(raw)} صف")
    print(f"🧹 بعد التنظيف: {len(clean_df)} صف")
    print(f"🔄 بعد إزالة التكرارات: {len(dedup_df)} صف")
    print(f"⚠️  صفوف شذوذ: {anomaly_report['anomalies_found']}")
    print(f"📈 النتيجة النهائية: {len(output_df)} صف")
    print("="*50)
    print(f"📁 البيانات النظيفة: {CLEAN_OUTPUT}")
    print(f"📁 البيانات النهائية: {DEDUP_OUTPUT}")
    print(f"📁 التقرير: {QUALITY_REPORT_PATH}")
    print("="*50 + "\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="معالج بيانات عام - يعمل مع أي CSV")
    parser.add_argument("--input", required=True, help="مسار ملف CSV")
    args = parser.parse_args()
    main(args.input)
