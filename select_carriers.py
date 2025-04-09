#!/usr/bin/env python3
import json
import requests
import os
from typing import Dict, List
import sys
import curses
from datetime import datetime

def fetch_carriers() -> List[Dict]:
    """Fetch carrier list from CSG API."""
    url = "https://csgapi.appspot.com/v1/med_supp/open/companies.json"
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error fetching carriers: {response.status_code}")
        return []

def load_selections() -> Dict[str, bool]:
    """Load existing carrier selections from config file."""
    if os.path.exists('carrier_selections.json'):
        with open('carrier_selections.json', 'r') as f:
            return json.load(f)
    return {}

def save_selections(selections: Dict[str, bool]):
    """Save carrier selections to config file."""
    with open('carrier_selections.json', 'w') as f:
        json.dump(selections, f, indent=2)

def draw_menu(stdscr, carriers: List[Dict], selections: Dict[str, bool], current_idx: int, offset: int):
    """Draw the carrier list with current selections."""
    height, width = stdscr.getmaxyx()
    
    # Clear screen
    stdscr.clear()
    
    # Draw header
    header = "Carrier Selection (Space to toggle, Up/Down to move, q to save and quit)"
    stdscr.addstr(0, 0, header[:width-1], curses.A_REVERSE)
    
    # Draw carriers
    for i in range(height - 2):  # -2 for header and status line
        carrier_idx = i + offset
        if carrier_idx >= len(carriers):
            break
            
        carrier = carriers[carrier_idx]
        naic = carrier['naic']
        selected = selections.get(naic, False)
        
        # Format carrier line
        line = f"[{'✓' if selected else ' '}] {carrier['name_full']} ({naic})"
        
        # Truncate line if too long
        if len(line) > width - 2:
            line = line[:width-5] + "..."
            
        # Highlight current line
        if carrier_idx == current_idx:
            stdscr.addstr(i + 1, 0, line[:width-1], curses.A_REVERSE)
        else:
            stdscr.addstr(i + 1, 0, line[:width-1])
    
    # Draw status line
    selected_count = sum(1 for selected in selections.values() if selected)
    status = f"Selected: {selected_count}/{len(carriers)} carriers"
    stdscr.addstr(height-1, 0, status[:width-1], curses.A_REVERSE)
    
    stdscr.refresh()

def select_carriers_ui(stdscr):
    """Interactive carrier selection process using curses."""
    # Set up curses
    curses.curs_set(0)  # Hide cursor
    stdscr.timeout(100)  # Non-blocking input
    
    # Get carriers and load existing selections
    carriers = fetch_carriers()
    if not carriers:
        return
    
    selections = load_selections()
    current_idx = 0
    offset = 0
    
    # Get screen dimensions
    height, width = stdscr.getmaxyx()
    max_display = height - 2  # -2 for header and status line
    
    while True:
        # Draw the menu
        draw_menu(stdscr, carriers, selections, current_idx, offset)
        
        # Get input
        try:
            key = stdscr.getch()
        except:
            continue
            
        if key == ord('q'):
            break
        elif key == ord(' '):
            # Toggle selection
            naic = carriers[current_idx]['naic']
            selections[naic] = not selections.get(naic, False)
        elif key == curses.KEY_UP and current_idx > 0:
            current_idx -= 1
            if current_idx < offset:
                offset = current_idx
        elif key == curses.KEY_DOWN and current_idx < len(carriers) - 1:
            current_idx += 1
            if current_idx >= offset + max_display:
                offset = current_idx - max_display + 1
    
    # Save selections
    save_selections(selections)
    
    # Show summary on exit
    stdscr.clear()
    selected_count = sum(1 for selected in selections.values() if selected)
    stdscr.addstr(0, 0, f"\nSelection Summary:")
    stdscr.addstr(1, 0, f"Total carriers: {len(carriers)}")
    stdscr.addstr(2, 0, f"Selected carriers: {selected_count}")
    stdscr.addstr(3, 0, "\nSelected carriers:")
    
    row = 4
    for carrier in carriers:
        if selections.get(carrier['naic'], False):
            if row < height - 1:  # Ensure we don't write past screen bottom
                stdscr.addstr(row, 0, f"✓ {carrier['name_full']} ({carrier['naic']})")
                row += 1
    
    stdscr.addstr(row + 1, 0, "\nPress any key to exit...")
    stdscr.refresh()
    stdscr.getch()

def main():
    try:
        curses.wrapper(select_carriers_ui)
    except KeyboardInterrupt:
        print("\nSelection cancelled.")
        sys.exit(1)

if __name__ == "__main__":
    main() 