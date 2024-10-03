import sys
from ui_elems.ChangeLogWindow import ChangeLogWindow
from PyQt5.QtWidgets import QApplication
if __name__ == '__main__':
    app = QApplication(sys.argv)
    window = ChangeLogWindow()
    sys.exit(app.exec_())