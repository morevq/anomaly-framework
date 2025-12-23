from PySide6.QtWidgets import (QComboBox, QSpinBox, QDoubleSpinBox, QFormLayout)
from .base_dialog import BaseAnomalyDialog


class TimeSeriesDialog(BaseAnomalyDialog):
    def __init__(self, columns, parent=None):
        super().__init__("Анализ временных рядов (Time Series)", parent)
        self.columns = columns

        form = QFormLayout()

        # 1. Колонка времени (обязательно)
        self.time_col_cb = QComboBox()
        self.time_col_cb.addItems(self.columns)
        form.addRow("Колонка времени (timestamp):", self.time_col_cb)

        # 2. Целевые колонки (можно несколько)
        # Для простоты в этом диалоге выберем одну, но SQL поддерживает массив
        self.target_col_cb = QComboBox()
        self.target_col_cb.addItems(["Все числовые"] + self.columns)
        form.addRow("Анализировать колонку:", self.target_col_cb)

        # 3. Размер окна (window_size)
        self.window_spin = QSpinBox()
        self.window_spin.setRange(2, 500)
        self.window_spin.setValue(7)
        form.addRow("Размер скользящего окна:", self.window_spin)

        # 4. Порог Z-score
        self.z_threshold_spin = QDoubleSpinBox()
        self.z_threshold_spin.setRange(1.0, 10.0)
        self.z_threshold_spin.setSingleStep(0.1)
        self.z_threshold_spin.setValue(3.0)
        form.addRow("Порог чувствительности (Z):", self.z_threshold_spin)

        # 5. Действие по исправлению
        self.action_cb = QComboBox()
        self.action_cb.addItems([
            "replace_with_rolling_mean",
            "nullify",
            "delete"
        ])
        form.addRow("Действие при фиксации:", self.action_cb)

        self.layout.addLayout(form)
        self.add_buttons()

    def save_and_accept(self, dry):
        target = self.target_col_cb.currentText()
        target_cols = None if target == "Все числовые" else [target]

        self.result_data = {
            "time_column": self.time_col_cb.currentText(),
            "target_columns": target_cols,
            "window_size": self.window_spin.value(),
            "z_threshold": self.z_threshold_spin.value(),
            "action": self.action_cb.currentText(),
            "dry_run": dry
        }
        self.accept()