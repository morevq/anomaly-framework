from PySide6.QtCore import Qt
from PySide6.QtWidgets import QListWidget, QListWidgetItem, QLabel, QCheckBox, QComboBox
from .base_dialog import BaseAnomalyDialog


class DuplicatesDialog(BaseAnomalyDialog):
    def __init__(self, columns, parent=None):
        super().__init__("Поиск и удаление дубликатов", parent)
        self.layout.addWidget(QLabel("Выберите колонки, составляющие уникальный ключ:"))

        self.list_widget = QListWidget()
        for col in columns:
            item = QListWidgetItem(col)
            item.setCheckState(Qt.Unchecked)
            self.list_widget.addItem(item)
        self.layout.addWidget(self.list_widget)

        self.keep_cb = QComboBox()
        self.keep_cb.addItems(["first", "last"])
        self.layout.addWidget(QLabel("Какую запись оставить?"))
        self.layout.addWidget(self.keep_cb)

        self.add_buttons()

    def save_and_accept(self, dry):
        selected_cols = [self.list_widget.item(i).text() for i in range(self.list_widget.count())
                         if self.list_widget.item(i).checkState() == Qt.Checked]

        self.result_data = {
            "target_columns": selected_cols if selected_cols else None,
            "keep": self.keep_cb.currentText(),
            "dry_run": dry
        }
        self.accept()