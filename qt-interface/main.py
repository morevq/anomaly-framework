import sys
import pandas as pd
from PySide6.QtWidgets import (QApplication, QMainWindow, QWidget, QVBoxLayout,
                               QPushButton, QFileDialog, QTableWidget,
                               QTableWidgetItem, QComboBox, QHeaderView,
                               QMessageBox, QCheckBox, QHBoxLayout, QLabel,
                               QDialog, QLineEdit, QFormLayout)
from PySide6.QtCore import QSettings
from sqlalchemy import ( create_engine, String, Float,
    MetaData, Table, Column, Text, Integer, Numeric,
    insert, select, cast, func, DateTime, Date, Boolean, Time
)

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
        self.quote_input = QLineEdit(';')
        self.quote_input.setFixedWidth(50)
        self.quote_input.textChanged.connect(self.reload_csv)
        csv_settings_layout.addWidget(self.quote_input)

        # Quote Character
        csv_settings_layout.addWidget(QLabel("Quote:"))
        self.delimiter_input = QLineEdit('"')
        self.delimiter_input.setFixedWidth(50)
        self.delimiter_input.textChanged.connect(self.reload_csv)
        csv_settings_layout.addWidget(self.delimiter_input)

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

        # self.btn_detect_anomalies = QPushButton("🔍 Поиск аномалий")
        # self.btn_detect_anomalies.clicked.connect(self.show_anomaly_dialog)
        # main_layout.addWidget(self.btn_detect_anomalies)

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
            null_val = self.null_input.text()

            header = 0 if self.has_header_cb.isChecked() else None

            # Читаем с учетом новых параметров
            self.df = pd.read_csv(
                self.current_file,
                sep=sep,
                header=header,
                quotechar=quote_char,
                escapechar=escape_char,
                na_values=null_val,
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
            print(guessed_type)
            combo.setCurrentText(guessed_type)

            self.mapping_table.setCellWidget(i, 2, combo)
    def process_import(self):
        if not self.engine:
            QMessageBox.warning(self, "Внимание", "Сначала настройте подключение к БД!")
            return

        target_name = self.table_name_input.text().strip()
        if not target_name:
            QMessageBox.warning(self, "Внимание", "Введите имя таблицы!")
            return

        metadata = MetaData()
        temp_name = f"temp_{target_name}"

        try:
            with self.engine.begin() as conn:
                # 1. Описываем и создаем временную таблицу через Core
                temp_cols = [
                    Column(self.mapping_table.item(i, 1).text(), Text)
                    for i in range(self.mapping_table.rowCount())
                ]
                temp_table = Table(temp_name, metadata, *temp_cols, extend_existing=True)
                temp_table.drop(conn, checkfirst=True)
                temp_table.create(conn)

                # 2. Загружаем DataFrame во временную таблицу
                rename_map = {
                    self.mapping_table.item(i, 0).text(): self.mapping_table.item(i, 1).text()
                    for i in range(self.mapping_table.rowCount())
                }
                self.df.rename(columns=rename_map).to_sql(
                    temp_name, conn, if_exists='append', index=False
                )

                # 3. Описываем и создаем основную таблицу
                final_cols = [Column("id", Integer, primary_key=True, autoincrement=True)]

                # ... внутри цикла формирования колонок в process_import ...
                for i in range(self.mapping_table.rowCount()):
                    col_name = self.mapping_table.item(i, 1).text().strip()
                    type_label = self.mapping_table.cellWidget(i, 2).currentText()

                    # Получаем КЛАСС или ОБЪЕКТ типа из нашего словаря
                    sql_type_class = SQL_TYPE_REGISTRY[type_label]

                    # Если это Numeric или String, можно добавить параметры инициализации, если нужно
                    # Для простоты здесь просто используем то, что в словаре
                    final_cols.append(Column(col_name, sql_type_class))

                target_table = Table(target_name, metadata, *final_cols, extend_existing=True)
                target_table.create(conn, checkfirst=True)

                null_placeholder = self.null_input.text()  # Берем значение из UI (например, '?')

                select_exprs = []
                target_cols_names = []

                for i in range(self.mapping_table.rowCount()):
                    sql_name = self.mapping_table.item(i, 1).text()
                    type_label = self.mapping_table.cellWidget(i, 2).currentText()
                    sql_type = SQL_TYPE_REGISTRY.get(type_label, Text)

                    target_cols_names.append(sql_name)

                    # ИСПОЛЬЗУЕМ null_placeholder из настроек UI
                    expr = cast(
                        func.nullif(temp_table.c[sql_name], null_placeholder),
                        sql_type
                    )
                    select_exprs.append(expr)

                # Строим сам запрос
                ins_query = insert(target_table).from_select(
                    [c.name for c in target_table.c if c.name != 'id'],  # Колонки куда вставляем
                    select(*select_exprs)  # Откуда берем
                )

                conn.execute(ins_query)

                # Удаляем временную таблицу
                temp_table.drop(conn)

            QMessageBox.information(self, "Успех", f"Данные импортированы в '{target_name}' через SQLAlchemy Core")

        except Exception as e:
            QMessageBox.critical(self, "Ошибка импорта", f"Ошибка: {str(e)}")


if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = CSVImporterApp()
    window.show()
    sys.exit(app.exec())