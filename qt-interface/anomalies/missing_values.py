from PySide6.QtWidgets import QTreeWidget, QTreeWidgetItem, QComboBox, QLineEdit, QHeaderView
from .base_dialog import BaseAnomalyDialog


class MissingValuesDialog(BaseAnomalyDialog):
    def __init__(self, columns, parent=None):
        super().__init__("Исправление пропусков (Missing Values)", parent)
        self.columns = columns

        self.tree = QTreeWidget()
        self.tree.setColumnCount(4)
        self.tree.setHeaderLabels(["Колонка", "Метод", "Значение", "Сортировка/Источник"])
        self.tree.header().setSectionResizeMode(QHeaderView.Stretch)

        for col in self.columns:
            item = QTreeWidgetItem([col])

            method_cb = QComboBox()
            method_cb.addItems(
                ["skip", "delete_row", "set_constant", "set_mode", "set_mean", "forward_fill", "backward_fill",
                 "copy_from_other_column"])

            other_col_cb = QComboBox()
            other_col_cb.addItems([""] + self.columns)

            val_input = QLineEdit()
            val_input.setEnabled(False)

            # Связываем активность полей с методом
            method_cb.currentTextChanged.connect(lambda t, vi=val_input, oc=other_col_cb:
                                                 (vi.setEnabled(t == "set_constant"),
                                                  oc.setEnabled("fill" in t or "column" in t)))

            self.tree.addTopLevelItem(item)
            self.tree.setItemWidget(item, 1, method_cb)
            self.tree.setItemWidget(item, 2, val_input)
            self.tree.setItemWidget(item, 3, other_col_cb)

        self.layout.addWidget(self.tree)
        self.add_buttons()

    def save_and_accept(self, dry):
        actions = {}
        for i in range(self.tree.topLevelItemCount()):
            item = self.tree.topLevelItem(i)
            col = item.text(0)
            method = self.tree.itemWidget(item, 1).currentText()
            if method == "skip": continue

            rule = {"method": method}
            if method == "set_constant": rule["value"] = self.tree.itemWidget(item, 2).text()
            if "fill" in method: rule["order_by"] = self.tree.itemWidget(item, 3).currentText()
            if "column" in method: rule["source_column"] = self.tree.itemWidget(item, 3).currentText()
            actions[col] = rule

        self.result_data = {"actions": actions, "dry_run": dry}
        self.accept()