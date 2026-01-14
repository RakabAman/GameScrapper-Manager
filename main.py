import os
import sys
import ctypes
from PyQt5.QtWidgets import QApplication
from gui import GameManager  # import your real GUI class

def get_base_dir():
    """Return the base directory for cache depending on run mode."""
    if getattr(sys, 'frozen', False):
        return os.path.dirname(sys.executable)
    else:
        return os.path.dirname(os.path.abspath(__file__))

def setup_cache():
    """Ensure a cache folder exists next to the exe/script."""
    base_dir = get_base_dir()
    cache_dir = os.path.join(base_dir, "cache")
    os.makedirs(cache_dir, exist_ok=True)
    return cache_dir

# In main.py, update hide_console_and_redirect function:
def hide_console_and_redirect():
    """Hide console window and setup logging to file."""
    import logging
    
    # Hide console
    ctypes.windll.user32.ShowWindow(ctypes.windll.kernel32.GetConsoleWindow(), 0)
    
    # Setup logging to file
    base_dir = get_base_dir()
    log_dir = os.path.join(base_dir, "logs")
    os.makedirs(log_dir, exist_ok=True)
    log_path = os.path.join(log_dir, "game_manager.log")
    
    # Configure logging
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        handlers=[
            logging.FileHandler(log_path, encoding='utf-8'),
            logging.StreamHandler(sys.stdout)  # Keep console output if console is shown
        ]
    )
    
    print(f"Logging to: {log_path}")


def main():
    # Setup logging first
    base_dir = get_base_dir()
    log_dir = os.path.join(base_dir, "logs")
    os.makedirs(log_dir, exist_ok=True)
    
    log_path = os.path.join(log_dir, "game_manager.log")
    
    # Tee output to both console and file
    class Tee:
        def __init__(self, *files):
            self.files = files
        def write(self, obj):
            for f in self.files:
                f.write(obj)
                f.flush()
        def flush(self):
            for f in self.files:
                f.flush()
    
    log_file = open(log_path, "w", encoding="utf-8")
    
    if "--show-console" not in sys.argv:
        ctypes.windll.user32.ShowWindow(ctypes.windll.kernel32.GetConsoleWindow(), 0)
        sys.stdout = Tee(log_file)
        sys.stderr = Tee(log_file)
    else:
        sys.stdout = Tee(sys.__stdout__, log_file)
        sys.stderr = Tee(sys.__stderr__, log_file)
    
    print(f"Application started. Log file: {log_path}")
    
    # Rest of your code...
    # Hide console unless explicitly requested
    if "--show-console" not in sys.argv:
        hide_console_and_redirect()

    # Prepare cache folder
    cache_dir = setup_cache()
    print(f"Cache folder ready at: {cache_dir}")

    # Launch your PyQt5 GUI
    app = QApplication(sys.argv)
    window = GameManager()
    window.show()
    sys.exit(app.exec_())

if __name__ == "__main__":
    main()
