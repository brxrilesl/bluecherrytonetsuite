from nicegui import ui

# Define a list to hold the board data
board = {
    'To Do': [],
    'In Progress': [],
    'Done': []
}

# Function to create a column with draggable items
def create_column(name, items):
    with ui.column().classes('w-1/3 p-2 border'):
        ui.label(name).classes('font-bold')
        for item in items:
            with ui.draggable().classes('p-2 m-2 border bg-white'):
                ui.label(item)

# Function to create the Trello board
def create_board():
    with ui.row().classes('space-x-4'):
        for column_name, items in board.items():
            create_column(column_name, items)

# Create the initial board
create_board()

ui.run()
