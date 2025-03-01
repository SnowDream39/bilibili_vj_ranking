import matplotlib.pyplot as plt
import numpy as np
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
import tkinter as tk
from tkinter import ttk
from math import ceil, floor

CONFIG = {
    "default_values": {
        "view": 10000,
        "favorite": 500,
        "coin": 500,
        "like": 500,
        "max_value": 100000,
        "copyright": 1
    },
    "plot_config": {
        "figsize": (8, 5),  
        "min_x": -100,
        "font_size": {
            "label": 10,   
            "title": 12,
            "legend": 9
        },
        "sample_points": 10000 
    },
    "gui_config": {
        "font": ('Helvetica', 10),
        "padding": "5",
        "label_widths": { 
            "basic": 20,   
            "rate": 20,     
            "point": 40,    
            "fix": 20      
        }
    }
}

class ScoreCalculator:
    @staticmethod
    def calculate_values(view, favorite, coin, like, copyright):
        copyright = 1 if copyright in [1, 3] else 2
        coin = 1 if (coin == 0 and view > 0 and favorite > 0 and like > 0) else coin
        fixA = 0 if coin <= 0 else (1 if copyright == 1 else ceil(max(1, (view + 20 * favorite + 40 * coin + 10 * like) / (200 * coin)) * 100) / 100)
        fixB = 0 if view + 20 * favorite <= 0 else ceil(min(1, 3 * max(0, (20 * coin + 10 * like)) / (view + 20 * favorite)) * 100) / 100
        fixC = 0 if like + favorite <= 0 else ceil(min(1, (like + favorite + 20 * coin * fixA)/(2 * like + 2 * favorite)) * 100) / 100
        
        viewR = 0 if view <= 0 else max(ceil(min(max((fixA * coin + favorite), 0) * 20 / view, 1) * 100) / 100, 0)
        favoriteR = 0 if favorite <= 0 else max(ceil(min((favorite + 2 * fixA * coin) * 10 / (favorite * 20 + view) * 40, 20) * 100) / 100, 0)
        coinR = 0 if fixA * coin * 40 + view <= 0 else max(ceil(min((fixA * coin * 40) / (fixA * coin * 40 + view) * 80, 40) * 100) / 100, 0)
        likeR = 0 if like <= 0 else max(floor(min(5, max(fixA * coin + favorite, 0) / (like * 20 + view) * 100) * 100) / 100, 0)
        
        viewP = view * viewR
        favoriteP = favorite * favoriteR
        coinP = coin * coinR
        likeP = like * likeR
        point = int(round(viewP + favoriteP + coinP * fixA + likeP) * fixB * fixC)
        
        return viewR, favoriteR, coinR, likeR, viewP, favoriteP, coinP, likeP, point, fixA, fixB, fixC

class PlotManager:
    def __init__(self, root):
        self.root = root
        self.setup_gui()
        self.create_plot()
        
    def setup_gui(self):
        self.main_frame = ttk.Frame(self.root, padding=CONFIG["gui_config"]["padding"])
        self.main_frame.grid(row=0, column=0, sticky="nsew")
        
        self.create_input_fields()
        self.create_copyright_selector()
        self.create_value_labels()
        self.create_controls()
        
    def create_input_fields(self):
        self.entries = {}
        fields = ["view", "favorite", "coin", "like"]
        
        for i, field in enumerate(fields):
            ttk.Label(self.main_frame, text=f"{field.title()}:", 
                     font=CONFIG["gui_config"]["font"]).grid(row=i, column=0, sticky="w")
            
            entry = ttk.Entry(self.main_frame, font=CONFIG["gui_config"]["font"])
            entry.insert(0, str(CONFIG["default_values"][field]))
            entry.grid(row=i, column=1, sticky="we")
            entry.bind("<Return>", self.update_plot)
            self.entries[field] = entry

    def create_copyright_selector(self):
        ttk.Label(self.main_frame, text="Copyright:", 
                 font=CONFIG["gui_config"]["font"]).grid(row=4, column=0, sticky="w")
        
        self.copyright_var = tk.IntVar(value=CONFIG["default_values"]["copyright"])
        copyright_frame = ttk.Frame(self.main_frame)
        copyright_frame.grid(row=4, column=1, sticky="we")
        
        ttk.Radiobutton(copyright_frame, text="1", variable=self.copyright_var, 
                       value=1, command=self.update_plot).grid(row=0, column=0)
        ttk.Radiobutton(copyright_frame, text="2", variable=self.copyright_var, 
                       value=2, command=self.update_plot).grid(row=0, column=1)

    def create_value_labels(self):
        self.value_frame = ttk.Frame(self.main_frame, padding=CONFIG["gui_config"]["padding"])
        self.value_frame.grid(row=5, column=0, columnspan=2, sticky="we")
        
        self.labels = {}
        label_groups = [
            [  
                [("view", "View"), ("viewR", "ViewR"), ("viewP", "ViewP")],
                [("favorite", "Fav"), ("favoriteR", "FavR"), ("favoriteP", "FavP")],
                [("coin", "Coin"), ("coinR", "CoinR"), ("coinP", "CoinP")],
                [("like", "Like"), ("likeR", "LikeR"), ("likeP", "LikeP")]
            ],
            [ 
                [("fixA", "FixA")],
                [("fixB", "FixB")],
                [("fixC", "FixC")],
                [("point", "Point")]
            ]
        ]
        
        left_frame = ttk.Frame(self.value_frame)
        left_frame.grid(row=0, column=0, padx=2)
        
        for row, group in enumerate(label_groups[0]):
            for col, (key, text) in enumerate(group):
                label = ttk.Label(
                    left_frame,
                    text=f"{text}: 0",
                    font=CONFIG["gui_config"]["font"],
                    width=12
                )
                label.grid(row=row, column=col, sticky="w", padx=2, pady=1)
                self.labels[key] = label
        
        right_frame = ttk.Frame(self.value_frame)
        right_frame.grid(row=0, column=1, padx=2)
        
        for row, group in enumerate(label_groups[1]):
            for key, text in group:
                label = ttk.Label(
                    right_frame,
                    text=f"{text}: 0",
                    font=CONFIG["gui_config"]["font"],
                    width=12
                )
                label.grid(row=row, column=0, sticky="w", padx=2, pady=1)
                self.labels[key] = label

    def create_controls(self):
        control_frame = ttk.Frame(self.main_frame)
        control_frame.grid(row=6, column=0, columnspan=2, sticky="we", pady=2)
        
        ttk.Label(control_frame, text="Max:", 
                 font=CONFIG["gui_config"]["font"]).grid(row=0, column=0, padx=2)
        self.max_value_entry = ttk.Entry(control_frame, font=CONFIG["gui_config"]["font"], width=8)
        self.max_value_entry.insert(0, str(CONFIG["default_values"]["max_value"]))
        self.max_value_entry.grid(row=0, column=1, padx=2)
        
        ttk.Label(control_frame, text="Var:", 
                 font=CONFIG["gui_config"]["font"]).grid(row=0, column=2, padx=2)
        self.variable_var = tk.StringVar()
        self.variable_combobox = ttk.Combobox(
            control_frame, 
            textvariable=self.variable_var,
            values=["View", "Favorite", "Coin", "Like"],
            state="readonly",
            font=CONFIG["gui_config"]["font"],
            width=8
        )
        self.variable_combobox.current(0)
        self.variable_combobox.grid(row=0, column=3, padx=2)
        
    def create_plot(self):
        self.fig = plt.Figure(figsize=CONFIG["plot_config"]["figsize"])
        self.canvas = FigureCanvasTkAgg(self.fig, master=self.root)
        self.canvas.draw()
        self.canvas.get_tk_widget().grid(row=1, column=0, columnspan=2, pady=10)
        self.update_plot()

    def update_plot(self, *args):
        try:
            values = self.get_current_values()
            self.plot_graph(values)
            self.update_labels(values)
        except ValueError:
            pass

    def get_current_values(self):
        return {
            'view': int(self.entries['view'].get()),
            'favorite': int(self.entries['favorite'].get()),
            'coin': int(self.entries['coin'].get()),
            'like': int(self.entries['like'].get()),
            'copyright': self.copyright_var.get(),
            'max_value': max(500, min(10000000, int(self.max_value_entry.get()))),
            'variable': self.variable_var.get()
        }

    def plot_graph(self, values):
        self.fig.clear()
        ax = self.fig.add_subplot(111)
        
        x = np.linspace(CONFIG["plot_config"]["min_x"], 
                       values['max_value'], 
                       CONFIG["plot_config"]["sample_points"])
        y = np.zeros_like(x)
        
        var_map = {'View': 'view', 'Favorite': 'favorite', 'Coin': 'coin', 'Like': 'like'}
        var_key = var_map[values['variable']]
        
        def calculate_point(x_val):
            temp_values = values.copy()
            temp_values[var_key] = x_val
            *_, point, _, _, _ = ScoreCalculator.calculate_values(
                temp_values['view'], temp_values['favorite'],
                temp_values['coin'], temp_values['like'],
                temp_values['copyright']
            )
            return point
        
        calculate_point_vec = np.vectorize(calculate_point)
        y = calculate_point_vec(x)
        
        ax.plot(x, y, label=f'Point vs {values["variable"]}', color='dodgerblue', linewidth=1.5)
        
        current_x = values[var_key]
        if CONFIG["plot_config"]["min_x"] <= current_x <= values['max_value']:
            *_, current_y, _, _, _ = ScoreCalculator.calculate_values(
                values['view'], values['favorite'],
                values['coin'], values['like'],
                values['copyright']
            )
            ax.plot(current_x, current_y, 'ro', markersize=4)
            ax.annotate(
                f'{values["variable"]}: {current_x}\nPoint: {current_y}',
                xy=(current_x, current_y),
                xytext=(current_x + 0.05 * (values['max_value'] - CONFIG["plot_config"]["min_x"]), current_y),
                arrowprops=dict(facecolor='black', shrink=0.05),
                fontsize=CONFIG["plot_config"]["font_size"]["label"],
                fontweight='bold'
            )
        
        title = f'Point vs {values["variable"]}'
        ax.set_title(title, fontsize=CONFIG["plot_config"]["font_size"]["title"])
        
        ax.set_xlabel(values['variable'], fontsize=CONFIG["plot_config"]["font_size"]["label"])
        ax.set_ylabel('Point', fontsize=CONFIG["plot_config"]["font_size"]["label"])
        ax.legend(fontsize=CONFIG["plot_config"]["font_size"]["legend"])
        ax.grid(True, alpha=0.3)
        
        self.fig.tight_layout()
        self.canvas.draw()
        
    def update_labels(self, values):
        results = ScoreCalculator.calculate_values(
            values['view'], values['favorite'],
            values['coin'], values['like'],
            values['copyright']
        )
    
        viewR, favoriteR, coinR, likeR, viewP, favoriteP, coinP, likeP, point, fixA, fixB, fixC = results
    
        updates = {
            'view': f"View: {values['view']}",
            'favorite': f"Fav: {values['favorite']}",
            'coin': f"Coin: {values['coin']}",
            'like': f"Like: {values['like']}",
            'viewR': f"ViewR: {viewR:.2f}",
            'favoriteR': f"FavR: {favoriteR:.2f}",
            'coinR': f"CoinR: {coinR:.2f}",
            'likeR': f"LikeR: {likeR:.2f}",
            'viewP': f"ViewP: {viewP:.0f}",
            'favoriteP': f"FavP: {favoriteP:.0f}",
            'coinP': f"CoinP: {coinP:.0f}",
            'likeP': f"LikeP: {likeP:.0f}",
            'fixA': f"FixA: {fixA:.2f}",
            'fixB': f"FixB: {fixB:.2f}",
            'fixC': f"FixC: {fixC:.2f}",
            'point': f"Point: {point:.0f}"
        }
    
        for key, text in updates.items():
            self.labels[key].config(text=text)

def main():
    root = tk.Tk()
    root.title("Interactive Plot")
    
    style = ttk.Style(root)
    style.theme_use('clam')
    
    app = PlotManager(root)
    root.mainloop()

if __name__ == "__main__":
    main()
