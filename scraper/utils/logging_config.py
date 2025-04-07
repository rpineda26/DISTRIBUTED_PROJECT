import logging
from colorama import init, Fore, Style
from tqdm import tqdm

init(autoreset=True)  # Initialize colorama

# Custom logger with colors
class ColoredLogger(logging.Logger):
    def __init__(self, name):
        super().__init__(name)
        
        # Create custom handler
        handler = logging.StreamHandler()
        handler.setFormatter(ColoredFormatter())
        self.addHandler(handler)
        
    def success(self, msg, *args, **kwargs):
        """Add success level logging"""
        self.log(25, msg, *args, **kwargs)  # 25 is between INFO and WARNING

class ColoredFormatter(logging.Formatter):
    def format(self, record):
        # Define color schemes
        colors = {
            'ERROR': Fore.RED,
            'WARNING': Fore.YELLOW,
            'INFO': Fore.WHITE,
            'SUCCESS': Fore.GREEN,
            'DEBUG': Fore.BLUE # Debug is not used as it doesn't show up if logging level is INFO
        }
        
        # Add timestamp and thread name to format
        msg = f"[{record.threadName}] {record.msg}"
        
        # Color code based on level
        level_name = record.levelname
        if level_name in colors:
            # Move cursor up above progress bars, print message, then restore cursor
            colored_msg = f"\033[2F\033[K{colors[level_name]}{msg}{Style.RESET_ALL}\033[2E"
            tqdm.write(colored_msg)
            return ""
        
        return msg

# Add success level to logging
logging.addLevelName(25, 'SUCCESS')
logging.setLoggerClass(ColoredLogger)