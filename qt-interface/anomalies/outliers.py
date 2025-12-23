from PySide6.QtWidgets import (QLabel, QComboBox, QDoubleSpinBox,
                               QLineEdit, QFormLayout)
from .base_dialog import BaseAnomalyDialog


class OutliersDialog(BaseAnomalyDialog):
    def __init__(self, columns, parent=None):
        super().__init__("Поиск и обработка выбросов (Outliers)", parent)
        self.columns = columns

        form = QFormLayout()

        # 1. Выбор колонки
        self.col_cb = QComboBox()
        self.col_cb.addItems(self.columns)
        form.addRow("Целевая колонка:", self.col_cb)

        # 2. Метод обнаружения
        self.method_cb = QComboBox()
        self.method_cb.addItems(["iqr", "zscore", "mad"])
        form.addRow("Метод обнаружения:", self.method_cb)

        # 3. Параметр чувствительности
        self.threshold_spin = QDoubleSpinBox()
        self.threshold_spin.setRange(0.1, 10.0)
        self.threshold_spin.setSingleStep(0.1)
        self.threshold_spin.setValue(1.5)
        form.addRow("Порог (k / threshold):", self.threshold_spin)

        self.method_cb.currentTextChanged.connect(self._update_threshold_default)

        # 4. Действие при исправлении (Маппинг: Красивое имя -> Техническое имя)
        self.actions_map = {
            "Выставить флаг": "flag",
            "Заменить на NULL": "nullify",
            "Заменить медианой": "replace_with_median",
            "Заменить средним": "replace_with_mean",
            "Ограничить (Cap)": "cap",
            "Удалить строку": "delete"
        }
        self.action_type_cb = QComboBox()
        self.action_type_cb.addItems(list(self.actions_map.keys()))
        form.addRow("Действие (для FIX):", self.action_type_cb)

        # 5. Имя колонки для флага
        self.flag_col_input = QLineEdit("is_outlier")
        form.addRow("Имя колонки флага:", self.flag_col_input)

        self.layout.addLayout(form)
        self.add_buttons()

    def _update_threshold_default(self, method):
        """Меняет стандартный порог в зависимости от метода"""
        if method == "iqr":
            self.threshold_spin.setValue(1.5)
        elif method == "zscore":
            self.threshold_spin.setValue(3.0)
        elif method == "mad":
            self.threshold_spin.setValue(3.5)

    def save_and_accept(self, dry):
        method = self.method_cb.currentText()
        val = self.threshold_spin.value()

        # Формируем JSON-параметры для SQL
        params = {
            "method": method,
            "flag_column": self.flag_col_input.text()
        }

        if method == "iqr":
            params["k"] = val
        else:
            params["threshold"] = val

        # Получаем техническое имя действия из нашего словаря
        display_action = self.action_type_cb.currentText()
        tech_action = self.actions_map.get(display_action, "flag")

        self.result_data = {
            "target_column": self.col_cb.currentText(),
            "action": tech_action,
            "params": params,
            "dry_run": dry
        }
        self.accept()