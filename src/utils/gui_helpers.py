"""
GUI Helpers Module

This module provides GUI utilities for the Canvas Downloader CLI application.
It creates professional popup dialogs for sensitive input (like API keys) while
maintaining the terminal-based interface for all other interactions.

Features:
- Secure password input dialog with masked text
- Professional window styling and positioning
- Graceful fallback to console input if GUI unavailable
- Cross-platform compatibility (Windows, macOS, Linux)
- Proper focus management and keyboard shortcuts
- Input validation and error handling

Usage:
    from ..utils.gui_helpers import get_secure_input

    api_key = get_secure_input(
        prompt="Enter your Canvas API Key:",
        title="Canvas API Key Required"
    )
"""

import sys
import os
from typing import Optional, Tuple
import threading
import time


def get_secure_input(prompt: str = "Enter secure value:",
                     title: str = "Secure Input Required",
                     placeholder: str = "Enter value here...") -> Optional[str]:
    """
    Display a GUI dialog for secure password input with fallback options.

    This function attempts to show a professional GUI dialog for password input.
    If the GUI is unavailable (no display, missing dependencies, etc.), it falls
    back to secure console input methods.

    Args:
        prompt: The prompt text to display in the dialog
        title: The window title
        placeholder: Placeholder text for the input field

    Returns:
        str: The entered value, or None if cancelled

    Raises:
        KeyboardInterrupt: If user cancels input
    """
    try:
        # Attempt GUI input first
        result = _show_password_dialog(prompt, title, placeholder)
        if result is not None:
            return result
        else:
            raise KeyboardInterrupt("User cancelled input")

    except ImportError:
        print("⚠️  GUI not available, using secure console input...")
        return _get_console_password_input(prompt)

    except Exception as e:
        print(f"⚠️  GUI error ({e}), falling back to console input...")
        return _get_console_password_input(prompt)


def _show_password_dialog(prompt: str, title: str, placeholder: str) -> Optional[str]:
    """
    Create and show a professional password input dialog using Tkinter.

    This creates a modal dialog with:
    - Centered positioning
    - Professional styling
    - Masked password input
    - Keyboard shortcuts (Enter to submit, Escape to cancel)
    - Input validation

    Args:
        prompt: The prompt text to display
        title: The window title
        placeholder: Placeholder text for the input field

    Returns:
        str: The entered password, or None if cancelled
    """
    try:
        import tkinter as tk
        from tkinter import ttk, messagebox
    except ImportError:
        raise ImportError("Tkinter not available")

    # Global variables to store result and dialog state
    result = {'value': None, 'submitted': False}

    def on_submit():
        """Handle form submission."""
        value = password_var.get().strip()
        if not value:
            messagebox.showerror("Error", "Please enter a value")
            password_entry.focus_set()
            return

        result['value'] = value
        result['submitted'] = True
        root.quit()

    def on_cancel():
        """Handle cancellation."""
        result['value'] = None
        result['submitted'] = True
        root.quit()

    def on_key_press(event):
        """Handle keyboard shortcuts."""
        if event.keysym == 'Return':
            on_submit()
        elif event.keysym == 'Escape':
            on_cancel()

    # Create the main window
    root = tk.Tk()
    root.title(title)
    root.resizable(False, False)

    # Configure window properties
    window_width = 450
    window_height = 200

    # Center the window
    screen_width = root.winfo_screenwidth()
    screen_height = root.winfo_screenheight()
    x = (screen_width - window_width) // 2
    y = (screen_height - window_height) // 2
    root.geometry(f"{window_width}x{window_height}+{x}+{y}")

    # Make window modal and always on top
    root.transient()
    root.grab_set()
    root.attributes('-topmost', True)

    # Configure style
    style = ttk.Style()
    style.theme_use('clam')  # Use a modern theme

    # Create main frame with padding
    main_frame = ttk.Frame(root, padding="20")
    main_frame.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))

    # Configure grid weights
    root.columnconfigure(0, weight=1)
    root.rowconfigure(0, weight=1)
    main_frame.columnconfigure(0, weight=1)

    # Add icon/logo area (optional)
    icon_frame = ttk.Frame(main_frame)
    icon_frame.grid(row=0, column=0, sticky=(tk.W, tk.E), pady=(0, 10))

    # Add Canvas Downloader label
    app_label = ttk.Label(icon_frame, text="🎓 Canvas Downloader",
                          font=('Arial', 12, 'bold'))
    app_label.pack()

    # Add prompt label
    prompt_label = ttk.Label(main_frame, text=prompt,
                             font=('Arial', 10), wraplength=400)
    prompt_label.grid(row=1, column=0, sticky=(tk.W, tk.E), pady=(0, 15))

    # Add password entry
    password_var = tk.StringVar()
    password_entry = ttk.Entry(main_frame, textvariable=password_var,
                               show="•", font=('Arial', 10), width=40)
    password_entry.grid(row=2, column=0, sticky=(tk.W, tk.E), pady=(0, 20))

    # Add placeholder text effect
    def on_focus_in(event):
        if password_var.get() == placeholder:
            password_var.set("")
            password_entry.config(show="•")

    def on_focus_out(event):
        if not password_var.get():
            password_entry.config(show="")
            password_var.set(placeholder)

    # Set initial placeholder
    password_entry.config(show="")
    password_var.set(placeholder)
    password_entry.bind('<FocusIn>', on_focus_in)
    password_entry.bind('<FocusOut>', on_focus_out)

    # Add buttons frame
    buttons_frame = ttk.Frame(main_frame)
    buttons_frame.grid(row=3, column=0, sticky=(tk.W, tk.E))
    buttons_frame.columnconfigure(0, weight=1)
    buttons_frame.columnconfigure(1, weight=1)

    # Add buttons
    cancel_button = ttk.Button(buttons_frame, text="Cancel", command=on_cancel)
    cancel_button.grid(row=0, column=0, sticky=(tk.W, tk.E), padx=(0, 5))

    submit_button = ttk.Button(buttons_frame, text="Submit", command=on_submit)
    submit_button.grid(row=0, column=1, sticky=(tk.W, tk.E), padx=(5, 0))

    # Style the submit button
    style.configure('Accent.TButton', font=('Arial', 10, 'bold'))
    submit_button.configure(style='Accent.TButton')

    # Bind keyboard events
    root.bind('<Key>', on_key_press)
    password_entry.bind('<Return>', lambda e: on_submit())
    password_entry.bind('<Escape>', lambda e: on_cancel())

    # Focus on password entry and clear placeholder
    root.after(100, lambda: (password_entry.focus_set(),
                             password_var.set("") if password_var.get() == placeholder else None,
                             password_entry.config(show="•") if password_var.get() != placeholder else None))

    # Handle window close button
    root.protocol("WM_DELETE_WINDOW", on_cancel)

    # Start the GUI event loop
    try:
        root.mainloop()
    finally:
        root.destroy()

    # Return the result
    return result['value'] if result['submitted'] else None


def _get_console_password_input(prompt: str) -> str:
    """
    Get secure password input from console with multiple fallback methods.

    Args:
        prompt: The prompt to display

    Returns:
        str: The entered password
    """
    try:
        # Try getpass (most secure)
        import getpass
        return getpass.getpass(f"{prompt} ")
    except Exception:
        try:
            # Try with masked input using msvcrt (Windows)
            if sys.platform == 'win32':
                import msvcrt
                print(f"{prompt} ", end='', flush=True)
                password = []
                while True:
                    char = msvcrt.getch()
                    if char in (b'\r', b'\n'):
                        print()
                        break
                    elif char == b'\x08':  # Backspace
                        if password:
                            password.pop()
                            print('\b \b', end='', flush=True)
                    else:
                        password.append(char.decode('utf-8'))
                        print('*', end='', flush=True)
                return ''.join(password)
        except Exception:
            pass

    # Last resort: regular input with warning
    print("⚠️  Warning: Input will be visible on screen!")
    return input(f"{prompt} ")


def test_gui_dialog():
    """Test function for the GUI dialog."""
    print("Testing GUI password dialog...")

    result = get_secure_input(
        prompt="Please enter your Canvas API key to test the dialog:",
        title="Canvas API Key - Test",
        placeholder="paste-your-api-key-here"
    )

    if result:
        print(f"✅ Success! Received {len(result)} characters")
        print(f"First 5 characters: {result[:5]}...")
    else:
        print("❌ Cancelled or no input received")


if __name__ == "__main__":
    test_gui_dialog()