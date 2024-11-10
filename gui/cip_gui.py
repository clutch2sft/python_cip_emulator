import tkinter as tk
from tkinter import messagebox
from tkinter.scrolledtext import ScrolledText
from core.cip_emulator import CIPEmulator
import threading
import queue
import json
from datetime import datetime
from utils.logger import create_logger
import gc, socket

class CIPGUI:
    def __init__(self, config_path):
        self.root = tk.Tk()
        self.root.title("CIP Emulator")

        # Load config
        self.config_path = config_path
        self.app_config, self.consumer_config, self.producers_config = self.load_config()
        self.hostname = socket.gethostname()
        # Initialize loggers
        self.server_logger = create_logger(f"{self.hostname }_server", log_to_console=False)
        self.client_logger = create_logger(f"{self.hostname }_client", log_to_console=False)

        # Initialize emulator with GUI mode
        self.emulator = CIPEmulator(
            app_config=self.app_config,
            producers_config=self.producers_config,
            consumer_config=self.consumer_config,
            logger_server=self.log_server_message,
            logger_client=self.log_client_message,
            gui_mode=True
        )

        # GUI setup and logging infrastructure
        self.setup_gui()
        self.server_log_queue = queue.Queue()
        # Initialize client log queues without clearing the dictionary on stop
        self.client_log_queues = {tag: queue.Queue() for tag in self.producers_config}
        self.client_log_windows = {}
        self.server_log_display = None  # Initialize to None to avoid AttributeError
        self.consumer_log_window = None
        self.server_running = threading.Event()
        self.client_running = threading.Event()
        self.log_update_lock = threading.Lock()

        # Periodic update for log display
        self.root.after(50, self.update_log_display)
        self.root.protocol("WM_DELETE_WINDOW", self.on_close)

    def load_config(self):
        with open(self.config_path, "r") as file:
            config = json.load(file)
        return config.get("app", {}), config.get("consumer", {}), config.get("producers", {})

    def save_config(self):
        config = {
            "app": self.app_config,
            "consumer": self.consumer_config,
            "producers": self.producers_config
        }
        with open(self.config_path, "w") as file:
            json.dump(config, file, indent=4)

    def setup_gui(self):
        self.start_server_button = tk.Button(self.root, text="Start Consumer", command=self.start_server)
        self.start_server_button.grid(row=0, column=0, padx=10, pady=5, sticky="ew")
        
        self.stop_server_button = tk.Button(self.root, text="Stop Consumer", command=self.stop_server, state=tk.DISABLED)
        self.stop_server_button.grid(row=0, column=1, padx=10, pady=5, sticky="ew")

        self.start_client_button = tk.Button(self.root, text="Start Producers", command=self.start_all_clients)
        self.start_client_button.grid(row=1, column=0, padx=10, pady=5, sticky="ew")
        
        self.stop_client_button = tk.Button(self.root, text="Stop Producers", command=self.stop_all_clients, state=tk.DISABLED)
        self.stop_client_button.grid(row=1, column=1, padx=10, pady=5, sticky="ew")

        self.edit_config_button = tk.Button(self.root, text="Edit Config", command=self.open_config_editor)
        self.edit_config_button.grid(row=2, column=0, columnspan=2, padx=10, pady=10, sticky="ew")

        self.root.grid_rowconfigure(3, weight=1)
        self.root.grid_columnconfigure(0, weight=1)
        self.root.grid_columnconfigure(1, weight=1)

    def open_config_editor(self):
        config_editor = tk.Toplevel(self.root)
        config_editor.title("Edit Configuration")
        # App and Consumer sections
        entries = {}
        for idx, (section, config) in enumerate([("App", self.app_config), ("Consumer", self.consumer_config)]):
            tk.Label(config_editor, text=f"{section} Config", font="bold").grid(row=idx, column=0, columnspan=2)
            for jdx, (key, value) in enumerate(config.items()):
                tk.Label(config_editor, text=key).grid(row=idx+jdx+1, column=0, padx=10, pady=5, sticky="w")
                entry = tk.Entry(config_editor)
                entry.insert(0, str(value))
                entry.grid(row=idx+jdx+1, column=1, padx=10, pady=5, sticky="ew")
                entries[(section, key)] = entry
        # Producer section
        tk.Label(config_editor, text="Producers Config", font="bold").grid(row=len(entries)+2, column=0, columnspan=2)
        for producer_tag, producer_config in self.producer_configs.items():
            tk.Label(config_editor, text=f"Producer: {producer_tag}").grid(columnspan=2)
            for kdx, (key, value) in enumerate(producer_config.items()):
                tk.Label(config_editor, text=key).grid(column=0, sticky="w")
                entry = tk.Entry(config_editor)
                entry.insert(0, str(value))
                entry.grid(column=1, padx=10, pady=5, sticky="ew")
                entries[(producer_tag, key)] = entry
        # Save and Cancel buttons
        def save_changes():
            # Update the configuration with new values from entries
            for (section, key), entry in entries.items():
                new_value = entry.get()
                try:
                    new_value = json.loads(new_value)  # Convert to the appropriate type
                except ValueError:
                    pass
                if section == "App":
                    self.app_config[key] = new_value
                elif section == "Consumer":
                    self.consumer_config[key] = new_value
                else:  # Producer configs
                    self.producer_configs[section][key] = new_value
            self.save_config()  # Save to file
            messagebox.showinfo("Configuration", "Configuration saved successfully.")
            config_editor.destroy()
        save_button = tk.Button(config_editor, text="Save", command=save_changes)
        save_button.grid(row=len(entries)+3, column=0, padx=10, pady=10, sticky="ew")
        cancel_button = tk.Button(config_editor, text="Cancel", command=config_editor.destroy)
        cancel_button.grid(row=len(entries)+3, column=1, padx=10, pady=10, sticky="ew")

    def log_server_message(self, message, level="INFO"):
        """Log a message for the server to both file and GUI, with correct log level."""
        timestamped_message = f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - {message}"

        # Log to the file with the appropriate log level
        log_method = getattr(self.server_logger, level.lower(), self.server_logger.info)
        log_method(message)

        # Add to GUI queue with color tagging
        gui_tag = "error" if level == "ERROR" else None
        self.server_log_queue.put((timestamped_message, gui_tag))


    def log_client_message(self, message, client_tag=None, level="INFO"):
        timestamped_message = f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - {message}"
        if client_tag and client_tag in self.client_log_queues:
            self.client_logger.info(f"{client_tag}: {message}")
            self.client_log_queues[client_tag].put((timestamped_message, "error" if level == "ERROR" else None))
        else:
            print(f"[ERROR] No log queue found for client '{client_tag}'")

    def start_server(self):
        if not self.server_running.is_set():
            self.server_running.set()
            self.consumer_log_window, self.server_log_display = self.create_log_window("Consumer Log")
            threading.Thread(target=self._start_server_thread, daemon=True).start()
            self.start_server_button.config(state=tk.DISABLED)
            self.stop_server_button.config(state=tk.NORMAL)
            self.update_edit_config_button_state()
            self.log_server_message("Attempting to start server...")

    def _start_server_thread(self):
        try:
            self.log_server_message("Starting server in background thread...")
            self.emulator.start_server()
            self.log_server_message("Server started successfully.")
        except Exception as e:
            self.log_server_message(f"Error starting server: {e}", level="ERROR")

    def stop_server(self):
        if self.server_running.is_set():
            self.server_running.clear()
            try:
                self.emulator.stop_server()
                self.log_server_message("Server stopped.")
            except Exception as e:
                self.log_server_message(f"Error stopping server: {e}", level="ERROR")
            finally:
                if self.consumer_log_window:
                    self.consumer_log_window.destroy()
                    self.consumer_log_window = None
                self.server_log_display = None
                self.start_server_button.config(state=tk.NORMAL)
                self.stop_server_button.config(state=tk.DISABLED)
                self.update_edit_config_button_state()

    def create_log_window(self, title, x_offset=0, y_offset=0):
        """Create a new log window with a ScrolledText widget, position it offset, and set close protocol."""
        log_window = tk.Toplevel(self.root)
        log_window.title(title)

        # Position each new log window with an offset
        main_x = self.root.winfo_x()
        main_y = self.root.winfo_y()
        main_width = self.root.winfo_width()
        main_height = self.root.winfo_height()

        log_window.geometry(f"+{main_x + main_width + x_offset}+{main_y + main_height + y_offset}")

        # Create and pack the ScrolledText widget
        log_display = ScrolledText(log_window, wrap="word", height=40, width=140, state="normal")
        log_display.pack(expand=True, fill="both")
        
        # Assign to either server or client log display based on the title
        if title == "Consumer Log":
            self.consumer_log_window = log_window
            self.server_log_display = log_display
        else:
            self.client_log_windows[title] = (log_window, log_display)

        # Set up a protocol to properly close the log window
        log_window.protocol("WM_DELETE_WINDOW", lambda: self._close_log_window(title))

        return log_window, log_display


    def _close_log_window(self, title):
        if title == "Consumer Log":
            if self.consumer_log_window:
                self.consumer_log_window.destroy()
                self.consumer_log_window = None
                self.server_log_display = None
        elif title in self.client_log_windows:
            log_window, _ = self.client_log_windows.pop(title, (None, None))
            if log_window:
                log_window.destroy()

    def start_all_clients(self):
        if not self.client_running.is_set():
            self.client_running.set()
            x_offset = 50
            y_offset = 50
            # Reset or initialize each queue for each client on start
            for client_tag in self.producers_config:
                self.client_log_queues[client_tag] = queue.Queue()  # Reset queue
                self.create_log_window(client_tag, x_offset=x_offset, y_offset=y_offset)
                x_offset = x_offset + 50
                y_offset = y_offset + 50

            threading.Thread(target=self._start_all_clients_thread, daemon=True).start()
            self.start_client_button.config(state=tk.DISABLED)
            self.stop_client_button.config(state=tk.NORMAL)

    def _start_all_clients_thread(self):
        try:
            self.emulator.start_all_clients()
            for client_tag in self.producers_config:
                self.log_client_message(message="Client started successfully", client_tag=client_tag)
        except Exception as e:
            for client_tag in self.producers_config:
                self.log_client_message(message=f"Error starting client: {e}", client_tag=client_tag, level="ERROR")

    def stop_all_clients(self):
        if self.client_running.is_set():
            self.client_running.clear()
            try:
                self.emulator.stop_all_clients()
                for client_tag in list(self.client_log_windows.keys()):
                    self._close_log_window(client_tag)
                print("All client log windows closed.")
                gc.collect()
            except Exception as e:
                print(f"Exception encountered while stopping clients: {e}")
            finally:
                self.start_client_button.config(state=tk.NORMAL)
                self.stop_client_button.config(state=tk.DISABLED)

    def update_log_display(self):
        with self.log_update_lock:
            if self.server_log_display:
                while not self.server_log_queue.empty():
                    message, tag = self.server_log_queue.get()
                    if tag == "error" and "error" not in self.server_log_display.tag_names():
                        self.server_log_display.tag_configure("error", foreground="red")
                    self.server_log_display.insert("end", message + "\n", tag if tag else None)
                    self.server_log_display.see("end")

            for client_tag, (_, log_display) in self.client_log_windows.items():
                while not self.client_log_queues[client_tag].empty():
                    message, tag = self.client_log_queues[client_tag].get()
                    if tag == "error" and "error" not in log_display.tag_names():
                        log_display.tag_configure("error", foreground="red")
                    log_display.insert("end", message + "\n", tag if tag else None)
                    log_display.see("end")

        self.root.after(50, self.update_log_display)

    def update_edit_config_button_state(self):
        if self.server_running.is_set() or self.client_running.is_set():
            self.edit_config_button.config(state=tk.DISABLED)
        else:
            self.edit_config_button.config(state=tk.NORMAL)

    def on_close(self):
        if self.client_running.is_set():
            self.stop_all_clients()
        if self.server_running.is_set():
            self.stop_server()
        self.root.destroy()

    def run(self):
        self.root.mainloop()
