import logging

# --- 1. ロギングの基本設定 ---
# これを最初に一度だけ呼び出します。
# level: どのレベル以上のログを記録するか (INFO, WARNING, ERRORなど)
# format: ログの出力形式
def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s | %(levelname)-8s | %(name)s | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )