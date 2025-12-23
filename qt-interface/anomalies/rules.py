import json
from PySide6.QtWidgets import (QTableWidget, QTableWidgetItem, QPushButton,
                               QVBoxLayout, QHBoxLayout, QComboBox, QHeaderView)
from .base_dialog import BaseAnomalyDialog


class RulesDialog(BaseAnomalyDialog):
    def __init__(self, columns, parent=None):
        super().__init__("Очистка на основе правил (Rule-based)", parent)
        self.columns = columns
        self.resize(800, 500)

        # Таблица правил
        self.rules_table = QTableWidget(0, 4)
        self.rules_table.setHorizontalHeaderLabels(["Название", "SQL Условие (EXPR)", "Действие", "Параметры"])
        self.rules_table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)

        # Кнопки управления списком
        btn_layout = QHBoxLayout()
        self.btn_add = QPushButton("➕ Добавить правило")
        self.btn_add.clicked.connect(self.add_rule_row)
        self.btn_remove = QPushButton("❌ Удалить выбранное")
        self.btn_remove.clicked.connect(self.remove_rule_row)
        btn_layout.addWidget(self.btn_add)
        btn_layout.addWidget(self.btn_remove)
        btn_layout.addStretch()

        self.layout.addLayout(btn_layout)
        self.layout.addWidget(self.rules_table)
        self.add_buttons()

    def add_rule_row(self):
        row = self.rules_table.rowCount()
        self.rules_table.insertRow(row)

        # Название (по умолчанию)
        self.rules_table.setItem(row, 0, QTableWidgetItem(f"rule_{row + 1}"))

        # SQL EXPR (подсказка)
        item_expr = QTableWidgetItem("column < 0")
        self.rules_table.setItem(row, 1, item_expr)

        # Выбор действия
        combo_action = QComboBox()
        combo_action.addItems(["report", "set_null", "set_value", "delete"])
        self.rules_table.setCellWidget(row, 2, combo_action)

        # Параметры (JSON или текст)
        self.rules_table.setItem(row, 3, QTableWidgetItem('{"column":"name", "value":"0"}'))

    def remove_rule_row(self):
        self.rules_table.removeRow(self.rules_table.currentRow())

    def save_and_accept(self, dry):
        rules = []
        for i in range(self.rules_table.rowCount()):
            name = self.rules_table.item(i, 0).text()
            expr = self.rules_table.item(i, 1).text()
            action = self.rules_table.cellWidget(i, 2).currentText()
            params_raw = self.rules_table.item(i, 3).text()

            rule = {
                "name": name,
                "expr": expr,
                "action": action
            }

            # Парсим параметры действия
            try:
                p_json = json.loads(params_raw)
                if action == "set_value":
                    rule["params"] = {"set_value": p_json}
                elif action == "set_null":
                    rule["params"] = {"target_columns": p_json.get("target_columns", [])}
            except:
                rule["params"] = {}

            rules.append(rule)

        self.result_data = {"rules": rules, "dry_run": dry}
        self.accept()