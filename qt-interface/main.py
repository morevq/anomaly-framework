import sys
import pandas as pd
from PySide6.QtWidgets import (QApplication, QMainWindow, QWidget, QVBoxLayout,
                               QPushButton, QFileDialog, QTableWidget,
                               QTableWidgetItem, QComboBox, QHeaderView,
                               QMessageBox, QCheckBox, QHBoxLayout, QLabel,
                               QDialog, QLineEdit, QFormLayout)
from PySide6.QtCore import QSettings, Qt
from sqlalchemy import (create_engine, String, Float,
                        MetaData, Table, Column, Text, Integer, Numeric,
                        insert, select, cast, func, DateTime, Date, Boolean, Time, case
                        )
from anomalies.missing_values import MissingValuesDialog
from anomalies.duplicates import DuplicatesDialog
from anomalies.outliers import OutliersDialog
from anomalies.rules import RulesDialog
from anomalies.timeseries import TimeSeriesDialog
import json
from sqlalchemy import text

# Типы данных для выбора
SQL_TYPES = {
    "Integer": Integer,
    "Float": Float,
    "String (255)": String(255),
    "Text": Text
}

SQL_TYPE_REGISTRY = {
    "Integer (Целое)": Integer,
    "Numeric(18, 2) (Точное число)": Numeric(18, 2),
    "Float (Число с плавающей точкой)": Float,
    "String (Строка 255)": String(255),
    "Text (Длинный текст)": Text,
    "DateTime (Дата и время)": DateTime,
    "Date (Дата)": Date,
    "Time (Время)": Time,
    "Boolean (Булево)": Boolean
}


class DbConfigDialog(QDialog):
    """Диалоговое окно для настройки подключения к PostgreSQL"""

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Настройки подключения к PostgreSQL")
        self.settings = QSettings("MyCompany", "CSVLoader")

        self.layout = QFormLayout(self)

        # Поля ввода
        self.host = QLineEdit(self.settings.value("db_host", "localhost"))
        self.port = QLineEdit(self.settings.value("db_port", "5432"))
        self.user = QLineEdit(self.settings.value("db_user", "postgres"))
        self.password = QLineEdit()
        self.password.setEchoMode(QLineEdit.EchoMode.Password)
        self.db_name = QLineEdit(self.settings.value("db_name", "my_database"))

        self.layout.addRow("Хост:", self.host)
        self.layout.addRow("Порт:", self.port)
        self.layout.addRow("Пользователь:", self.user)
        self.layout.addRow("Пароль:", self.password)
        self.layout.addRow("Имя БД:", self.db_name)

        # Кнопки
        self.btn_save = QPushButton("Подключиться")
        self.btn_save.clicked.connect(self.accept)
        self.layout.addRow(self.btn_save)

    def get_connection_string(self):
        # Сохраняем настройки (кроме пароля)
        self.settings.setValue("db_host", self.host.text())
        self.settings.setValue("db_port", self.port.text())
        self.settings.setValue("db_user", self.user.text())
        self.settings.setValue("db_name", self.db_name.text())
        self.settings.setValue("db_pass", self.password.text())
        return (f"postgresql+psycopg2://{self.user.text()}:{self.password.text()}@"
                f"{self.host.text()}:{self.port.text()}/{self.db_name.text()}")

class CSVImporterApp(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("CSV to PostgreSQL Importer")
        self.resize(1100, 750)

        self.df = None
        self.engine = None
        self.current_file = None  # Запоминаем путь к файлу
        self.init_ui()

    def init_ui(self):
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        main_layout = QVBoxLayout(central_widget)

        # Панель инструментов
        toolbar = QHBoxLayout()

        self.btn_config = QPushButton("⚙ Настроить БД")
        self.btn_config.clicked.connect(self.open_config)

        # Настройка CSV
        self.has_header_cb = QCheckBox("Есть заголовки")
        self.has_header_cb.setChecked(True)
        self.has_header_cb.stateChanged.connect(self.reload_csv)  # Перезагрузить при смене

        toolbar.addWidget(self.btn_config)
        toolbar.addSpacing(20)
        toolbar.addWidget(self.has_header_cb)

        toolbar.addStretch()
        self.btn_open = QPushButton("📁 Открыть CSV")
        self.btn_open.clicked.connect(self.select_file)
        toolbar.addWidget(self.btn_open)

        main_layout.addLayout(toolbar)

        self.status_label = QLabel("Статус: Ожидание подключения...")
        main_layout.addWidget(self.status_label)

        # Таблица маппинга
        self.mapping_table = QTableWidget()
        self.mapping_table.setColumnCount(3)
        self.mapping_table.setHorizontalHeaderLabels(["Колонка в CSV", "Имя в SQL", "Тип данных"])
        self.mapping_table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
        main_layout.addWidget(self.mapping_table)

        # Поле для названия новой таблицы
        self.table_name_input = QLineEdit()
        self.table_name_input.setPlaceholderText("Введите название таблицы (например, users_import)")
        main_layout.addWidget(QLabel("Название создаваемой таблицы:"))
        main_layout.addWidget(self.table_name_input)

        self.btn_execute = QPushButton("🚀 Запустить импорт")
        self.btn_execute.clicked.connect(self.process_import)
        self.btn_execute.setEnabled(False)
        self.btn_execute.setFixedHeight(50)
        main_layout.addWidget(self.btn_execute)

        csv_settings_layout = QHBoxLayout()

        csv_settings_layout.addWidget(QLabel("Delimiter:"))
        self.delimiter_input = QLineEdit(',')
        self.delimiter_input.setFixedWidth(50)
        self.delimiter_input.textChanged.connect(self.reload_csv)
        csv_settings_layout.addWidget(self.delimiter_input)

        # Quote Character
        csv_settings_layout.addWidget(QLabel("Quote:"))
        self.quote_input = QLineEdit('"')
        self.quote_input.setFixedWidth(50)
        self.quote_input.textChanged.connect(self.reload_csv)
        csv_settings_layout.addWidget(self.quote_input)

        # Escape Character
        csv_settings_layout.addWidget(QLabel("Escape:"))
        self.escape_input = QLineEdit("'")
        self.escape_input.setFixedWidth(50)
        self.escape_input.textChanged.connect(self.reload_csv)
        csv_settings_layout.addWidget(self.escape_input)

        # NULL Strings
        csv_settings_layout.addWidget(QLabel("NULL String:"))
        self.null_input = QLineEdit("?")  # По умолчанию ваш '?'
        self.null_input.setFixedWidth(50)
        self.null_input.textChanged.connect(self.reload_csv)
        csv_settings_layout.addWidget(self.null_input)

        csv_settings_layout.addStretch()
        main_layout.insertLayout(2, csv_settings_layout)

        self.btn_fix_missing = QPushButton("🧩 Исправить пропуски")
        self.btn_fix_missing.clicked.connect(self.run_missing_fix)  # Метод уже обсуждали выше

        self.btn_fix_dupes = QPushButton("👯 Найти дубликаты")
        self.btn_fix_dupes.clicked.connect(self.run_duplicates_fix)  # Метод уже обсуждали выше

        # Добавляем их в layout
        anomaly_layout = QHBoxLayout()
        anomaly_layout.addWidget(self.btn_fix_missing)
        anomaly_layout.addWidget(self.btn_fix_dupes)
        main_layout.addLayout(anomaly_layout)

        self.btn_fix_outliers = QPushButton("📉 Обработка выбросов")
        self.btn_fix_outliers.clicked.connect(self.run_outliers_fix)
        anomaly_layout.addWidget(self.btn_fix_outliers)

        self.btn_rule_based = QPushButton("⚖️ Правила очистки")
        self.btn_rule_based.clicked.connect(self.run_rule_based_fix)
        anomaly_layout.addWidget(self.btn_rule_based)

        self.btn_timeseries = QPushButton("📈 Временные ряды")
        self.btn_timeseries.clicked.connect(self.run_timeseries_fix)
        anomaly_layout.addWidget(self.btn_timeseries)

    def open_config(self):
        dialog = DbConfigDialog(self)
        if dialog.exec():
            conn_str = dialog.get_connection_string()
            try:
                self.engine = create_engine(conn_str)
                # Проверка соединения
                with self.engine.connect() as conn:
                    self.status_label.setText("Статус: ✅ Подключено к PostgreSQL")
                    QMessageBox.information(self, "Успех", "Соединение с БД установлено!")
            except Exception as e:
                self.engine = None
                self.status_label.setText("Статус: ❌ Ошибка подключения")
                QMessageBox.critical(self, "Ошибка", f"Не удалось подключиться: {str(e)}")

    def select_file(self):
        file_path, _ = QFileDialog.getOpenFileName(self, "Открыть CSV", "",
                                                   "CSV Files (*.csv);;Text Files (*.txt);;All Files (*)")
        if file_path:
            self.current_file = file_path
            self.reload_csv()

    def reload_csv(self):
        if not self.current_file:
            return

        try:
            sep = self.delimiter_input.text()

            # Получаем новые настройки из UI
            quote_char = self.quote_input.text() or '"'
            escape_char = self.escape_input.text() or None
            null_input_text = self.null_input.text()
            null_values_list = (x.strip() for x in null_input_text.split(',')) if null_input_text else ()

            header = 0 if self.has_header_cb.isChecked() else None

            # Читаем с учетом новых параметров
            self.df = pd.read_csv(
                self.current_file,
                sep=sep,
                header=header,
                quotechar=quote_char,
                escapechar=escape_char,
                na_values=null_values_list,
                keep_default_na=True,
                encoding='utf-8-sig'
            )

            if not self.has_header_cb.isChecked():
                self.df.columns = [f"col_{i + 1}" for i in range(len(self.df.columns))]

            self.update_mapping_table()
            self.btn_execute.setEnabled(True)
            self.status_label.setText(f"Файл загружен. Строк: {len(self.df)}")

        except Exception as e:
            QMessageBox.warning(self, "Ошибка парсинга", f"Ошибка: {str(e)}")

    def guess_sql_type(self, pandas_dtype):
        """Определяет наиболее вероятный SQL тип на основе данных pandas"""
        import pandas as pd

        if pd.api.types.is_datetime64_any_dtype(pandas_dtype):
            return "DateTime (Дата и время)"
        elif pd.api.types.is_integer_dtype(pandas_dtype):
            return "Integer (Целое)"
        elif pd.api.types.is_float_dtype(pandas_dtype):
            return "Numeric (Точное число)"
        elif pd.api.types.is_bool_dtype(pandas_dtype):
            return "Boolean (Булево)"
        else:
            # Проверка: вдруг это строка, которая на самом деле дата?
            # (Опционально можно добавить попытку pd.to_datetime)
            return "String (Строка 255)"

    def update_mapping_table(self):
        self.mapping_table.setRowCount(len(self.df.columns))
        for i, col in enumerate(self.df.columns):
            # 1. Оригинальное имя
            self.mapping_table.setItem(i, 0, QTableWidgetItem(str(col)))

            # 2. Предлагаемое имя в SQL (очистка от спецсимволов)
            clean_name = "".join([c if c.isalnum() else "_" for c in str(col)]).lower()
            self.mapping_table.setItem(i, 1, QTableWidgetItem(clean_name))

            # 3. Выпадающий список типов
            combo = QComboBox()
            combo.addItems(SQL_TYPE_REGISTRY.keys())

            # АВТООПРЕДЕЛЕНИЕ ТИПА
            guessed_type = self.guess_sql_type(self.df[col].dtype)
            combo.setCurrentText(guessed_type)

            self.mapping_table.setCellWidget(i, 2, combo)

    def process_import(self):
        if not self.engine:
            QMessageBox.critical(self, "Ошибка", "Нет подключения к БД")
            return

        target_name = self.table_name_input.text().strip()
        if not target_name:
            QMessageBox.warning(self, "Внимание", "Введите имя таблицы")
            return

        # 1. Подготовка параметров NULL
        null_input_text = self.null_input.text()
        null_strings = [x.strip() for x in null_input_text.split(',')] if null_input_text else []

        metadata = MetaData()
        temp_name = f"temp_{target_name}"

        try:
            with self.engine.begin() as conn:
                # --- ШАГ 1: Создание временной (staging) таблицы ---
                # В ней все колонки имеют тип Text для первичной загрузки
                temp_cols = []
                for i in range(self.mapping_table.rowCount()):
                    sql_name = self.mapping_table.item(i, 1).text()
                    temp_cols.append(Column(sql_name, Text))

                temp_table = Table(temp_name, metadata, *temp_cols, extend_existing=True)
                temp_table.drop(conn, checkfirst=True)
                temp_table.create(conn)

                # --- ШАГ 2: Загрузка данных из DataFrame в Staging ---
                # Переименовываем колонки в DF согласно маппингу
                rename_map = {
                    self.mapping_table.item(i, 0).text(): self.mapping_table.item(i, 1).text()
                    for i in range(self.mapping_table.rowCount())
                }
                upload_df = self.df.rename(columns=rename_map)
                upload_df.to_sql(temp_name, conn, if_exists='append', index=False)

                # --- ШАГ 3: Формирование структуры финальной таблицы ---
                final_cols = [Column("id", Integer, primary_key=True)]  # Наш системный PK
                target_cols_names = []  # Список имен для INSERT
                select_exprs = []  # Список выражений для SELECT

                for i in range(self.mapping_table.rowCount()):
                    orig_name = self.mapping_table.item(i, 0).text()
                    sql_name = self.mapping_table.item(i, 1).text()

                    # Пропускаем id из CSV, так как у нас есть свой PK
                    if sql_name.lower() == 'id':
                        continue

                    type_label = self.mapping_table.cellWidget(i, 2).currentText()

                    sql_type = SQL_TYPE_REGISTRY.get(type_label, Text)

                    # Добавляем колонку в схему финальной таблицы
                    final_cols.append(Column(sql_name, sql_type))

                    # Добавляем в списки для вставки (синхронно!)
                    target_cols_names.append(sql_name)

                    # Выражение CAST(NULLIF(col, ?) AS type) с поддержкой нескольких NULL
                    select_exprs.append(
                        cast(
                            case(
                                (temp_table.c[sql_name].in_(null_strings), None),
                                else_=temp_table.c[sql_name]
                            ),
                            sql_type
                        ).label(sql_name)
                    )

                # --- ШАГ 4: Создание финальной таблицы и переливка данных ---
                target_table = Table(target_name, metadata, *final_cols, extend_existing=True)
                target_table.drop(conn, checkfirst=True)  # Опционально: удалять ли старую таблицу
                target_table.create(conn)

                # Выполнение вставки через SELECT
                # Здесь target_cols_names и select_exprs имеют одинаковую длину
                ins_query = insert(target_table).from_select(
                    target_cols_names,

                    select(*select_exprs)
                )

                conn.execute(ins_query)

                # Удаляем временную таблицу
                temp_table.drop(conn)

            QMessageBox.information(self, "Успех", f"Данные успешно импортированы в таблицу '{target_name}'")

        except Exception as e:
            QMessageBox.critical(self, "Ошибка импорта", f"Критическая ошибка: {str(e)}")

    def show_audit_info(self, result_json):
        """Парсинг ответа от функций anomaly_detect/fix и вывод отчета"""
        if not result_json:
            QMessageBox.warning(self, "Предупреждение", "Функция не вернула данных.")
            return

        # Извлекаем основные поля из JSONB (который пришел как словарь Python)
        audit_id = result_json.get('audit_id')
        kind = result_json.get('kind', 'unknown')
        mode = result_json.get('mode', 'process')
        dry_run = result_json.get('dry_run', False)

        # Определяем количество затронутых строк/групп в зависимости от типа функции
        count = result_json.get('groups_processed') or result_json.get('rows_affected') or 0

        status_str = "🧪 СУХОЙ ПРОГОН (изменения не внесены)" if dry_run else "🚀 УСПЕШНО ВЫПОЛНЕНО"

        report = [
            f"<b>Статус:</b> {status_str}",
            f"<b>Тип операции:</b> {kind} ({mode})",
            f"<b>ID Аудита:</b> {audit_id}",
            f"<b>Обработано объектов:</b> {count}",
            "<br><i>Детальный лог сохранен в таблицах dedup_audit и dedup_audit_rows.</i>"
        ]

        msg_box = QMessageBox(self)
        msg_box.setWindowTitle("Отчет по аномалиям")
        msg_box.setTextFormat(Qt.RichText)  # Чтобы работал <b> и <br>
        msg_box.setText("<br>".join(report))
        msg_box.setIcon(QMessageBox.Information if not dry_run else QMessageBox.Question)
        msg_box.exec()

    def run_missing_fix(self):
        # 1. Считываем имя таблицы из поля ввода
        target_table = self.table_name_input.text().strip()

        if not target_table:
            QMessageBox.warning(self, "Внимание", "Укажите имя таблицы, которую нужно обработать.")
            return

        # 2. Получаем список колонок из таблицы маппинга (или напрямую из БД)
        cols = [self.mapping_table.item(i, 1).text() for i in range(self.mapping_table.rowCount())]

        # 3. Открываем диалог настроек
        dlg = MissingValuesDialog(cols, self)
        if dlg.exec():
            data = dlg.result_data
            print(text(
                        "SELECT anomaly_fix_missing(:s, :t, :p_cols, :k_cols, NULL, :params, :dry)"
                    ), {
                        "s": "public",
                        "t": target_table,  # Имя из поля ввода
                        "p_cols": None,
                        "k_cols": None,
                        "params": json.dumps({"actions": data["actions"]}),
                        "dry": data["dry_run"]
                    })
            try:
                with self.engine.begin() as conn:
                    # Вызываем функцию для указанной таблицы (target_table)
                    res = conn.execute(text(
                        "SELECT anomaly_fix_missing(:s, :t, :p_cols, :k_cols, NULL, :params, :dry)"
                    ), {
                        "s": "public",
                        "t": target_table,  # Имя из поля ввода
                        "p_cols": None,
                        "k_cols": None,
                        "params": json.dumps({"actions": data["actions"]}),
                        "dry": data["dry_run"]
                    }).scalar()
                    self.show_audit_info(res)
            except Exception as e:
                QMessageBox.critical(self, "Ошибка SQL", f"Не удалось обработать таблицу '{target_table}':\n{str(e)}")

    def run_duplicates_fix(self):
        target_table = self.table_name_input.text().strip()

        if not target_table:
            QMessageBox.warning(self, "Внимание", "Укажите имя таблицы для поиска дубликатов.")
            return

        cols = [self.mapping_table.item(i, 1).text() for i in range(self.mapping_table.rowCount())]

        dlg = DuplicatesDialog(cols, self)
        if dlg.exec():
            data = dlg.result_data
            print(text(
                        "SELECT anomaly_fix_duplicates(:s, :t, :p_cols, NULL, 'delete', :params, :dry)"
                    ), {
                        "s": "public",
                        "t": target_table,  # Имя из поля ввода
                        "p_cols": data["target_columns"],
                        "params": json.dumps({"keep": data["keep"]}),
                        "dry": data["dry_run"]
                    })
            try:
                with self.engine.begin() as conn:
                    res = conn.execute(text(
                        "SELECT anomaly_fix_duplicates(:s, :t, :p_cols, NULL, 'delete', :params, :dry)"
                    ), {
                        "s": "public",
                        "t": target_table,  # Имя из поля ввода
                        "p_cols": data["target_columns"],
                        "params": json.dumps({"keep": data["keep"]}),
                        "dry": data["dry_run"]
                    }).scalar()
                    self.show_audit_info(res)
            except Exception as e:
                QMessageBox.critical(self, "Ошибка SQL",
                                     f"Ошибка при удалении дубликатов в '{target_table}':\n{str(e)}")

    def run_outliers_fix(self):
        target_table = self.table_name_input.text().strip()
        if not target_table:
            QMessageBox.warning(self, "Внимание", "Укажите имя таблицы.")
            return

        cols = [self.mapping_table.item(i, 1).text() for i in range(self.mapping_table.rowCount())]
        dlg = OutliersDialog(cols, self)

        if dlg.exec():
            data = dlg.result_data
            try:
                with self.engine.begin() as conn:
                    print(text(
                        "SELECT anomaly_fix_outliers(:s, :t, :p_cols, :k_cols, :action, :params, :dry)"
                    ), {
                        "s": "public",
                        "t": target_table,
                        "p_cols": [data["target_column"]],  # Оборачиваем в список для ARRAY
                        "k_cols": ["id"],  # Предполагаем, что id всегда есть
                        "action": data["action"],
                        "params": json.dumps(data["params"]),
                        "dry": data["dry_run"]
                    })
                    res = conn.execute(text(
                        "SELECT anomaly_fix_outliers(:s, :t, :p_cols, :k_cols, :action, :params, :dry)"
                    ), {
                        "s": "public",
                        "t": target_table,
                        "p_cols": [data["target_column"]],  # Оборачиваем в список для ARRAY
                        "k_cols": ["id"],  # Предполагаем, что id всегда есть
                        "action": data["action"],
                        "params": json.dumps(data["params"]),
                        "dry": data["dry_run"]
                    }).scalar()
                    self.show_audit_info(res)
            except Exception as e:
                QMessageBox.critical(self, "Ошибка SQL", f"Ошибка при обработке выбросов:\n{str(e)}")

    def run_rule_based_fix(self):
        target_table = self.table_name_input.text().strip()
        if not target_table:
            QMessageBox.warning(self, "Внимание", "Укажите имя таблицы.")
            return

        cols = [self.mapping_table.item(i, 1).text() for i in range(self.mapping_table.rowCount())]
        dlg = RulesDialog(cols, self)

        if dlg.exec():
            data = dlg.result_data
            try:
                with self.engine.begin() as conn:
                    res = conn.execute(text(
                        "SELECT anomaly_fix_rule_based(:s, :t, NULL, :k_cols, NULL, :params, :dry)"
                    ), {
                        "s": "public",
                        "t": target_table,
                        "k_cols": ["id"],
                        "params": json.dumps({"rules": data["rules"]}),
                        "dry": data["dry_run"]
                    }).scalar()
                    self.show_audit_info(res)
            except Exception as e:
                QMessageBox.critical(self, "Ошибка SQL", f"Ошибка Rule-based очистки:\n{str(e)}")

    def run_timeseries_fix(self):
        target_table = self.table_name_input.text().strip()
        if not target_table:
            QMessageBox.warning(self, "Внимание", "Укажите имя таблицы.")
            return

        # Получаем список колонок для диалога
        cols = [self.mapping_table.item(i, 1).text() for i in range(self.mapping_table.rowCount())]
        dlg = TimeSeriesDialog(cols, self)

        if dlg.exec():
            data = dlg.result_data
            print(text(
                        "SELECT anomaly_fix_timeseries(:s, :t, :p_cols, :k_cols, :action, :params, :dry)"
                    ), {
                        "s": "public",
                        "t": target_table,
                        "p_cols": data["target_columns"],  # ARRAY или NULL
                        "k_cols": ["id"],  # Используем id как ключ
                        "action": data["action"],
                        "params": json.dumps({
                            "time_column": data["time_column"],
                            "window_size": data["window_size"],
                            "z_threshold": data["z_threshold"]
                        }),
                        "dry": data["dry_run"]
                    })
            try:
                with self.engine.begin() as conn:
                    # Вызываем функцию исправления (она внутри вызывает логику поиска)
                    res = conn.execute(text(
                        "SELECT anomaly_fix_timeseries(:s, :t, :p_cols, :k_cols, :action, :params, :dry)"
                    ), {
                        "s": "public",
                        "t": target_table,
                        "p_cols": data["target_columns"],  # ARRAY или NULL
                        "k_cols": ["id"],  # Используем id как ключ
                        "action": data["action"],
                        "params": json.dumps({
                            "time_column": data["time_column"],
                            "window_size": data["window_size"],
                            "z_threshold": data["z_threshold"]
                        }),
                        "dry": data["dry_run"]
                    }).scalar()
                    self.show_audit_info(res)
            except Exception as e:
                QMessageBox.critical(self, "Ошибка SQL", f"Ошибка анализа временного ряда:\n{str(e)}")

if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = CSVImporterApp()
    window.show()
    sys.exit(app.exec())