import logging
import curses

# https://stackoverflow.com/questions/27774093/how-to-manage-logging-in-curses

class CursesHandler(logging.Handler):
    def __init__(self, screen):
        super().__init__()
        self.screen = screen
        
    def emit(self, record):
        try:
            msg = self.format(record)
            self.screen.addstr(f"\n{msg}")
            self.screen.refresh()

        except (KeyboardInterrupt, SystemExit):
            raise
        except:
            self.handleError(record)