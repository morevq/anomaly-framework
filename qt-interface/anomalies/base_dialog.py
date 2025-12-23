from PySide6.QtWidgets import QDialog, QVBoxLayout, QHBoxLayout, QPushButton


class BaseAnomalyDialog(QDialog):
    def __init__(self, title, parent=None):
        super().__init__(parent)
        self.setWindowTitle(title)
        self.setMinimumSize(600, 400)
        self.layout = QVBoxLayout(self)
        self.result_data = None

    def add_buttons(self):
        btn_box = QHBoxLayout()
        self.btn_dry = QPushButton("🧪 Сухой прогон")
        self.btn_run = QPushButton("🚀 Применить")
        self.btn_run.setStyleSheet("background-color: #d9534f; color: white;")

        btn_box.addStretch()
        btn_box.addWidget(self.btn_dry)
        btn_box.addWidget(self.btn_run)
        self.layout.addLayout(btn_box)

        self.btn_dry.clicked.connect(lambda: self.save_and_accept(dry=True))
        self.btn_run.clicked.connect(lambda: self.save_and_accept(dry=False))

    def save_and_accept(self, dry):
        # Переопределяется в наследниках
        pass