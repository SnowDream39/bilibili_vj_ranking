import matplotlib.pyplot as plt
import numpy as np
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
import tkinter as tk
from tkinter import ttk

def calculate_values(view, favorite, coin, like, hascopyright):
    viewR = 0 if view == 0 else max(np.ceil(np.clip((coin + favorite) * 20 / view, 0, 1) * 100) / 100, 0)
    favoriteR = 0 if favorite <= 0 else max(np.ceil(np.clip((favorite + 2 * coin) * 10 / (favorite * 20 + view) * 40, 0, 20) * 100) / 100, 0)
    coinR = 0 if (1 if hascopyright == 1 else 2) * coin * 40 + view == 0 else max(np.ceil(np.clip(((1 if hascopyright == 1 else 2) * coin * 40) / ((1 if hascopyright == 1 else 2) * coin * 40 + view) * 80, 0, 40) * 100) / 100, 0)
    likeR = 0 if like <= 0 else max(np.floor(np.clip(coin + favorite, 0, None) / (like * 20 + view) * 100 * 100) / 100, 0)
    
    viewP = round(view * viewR)
    favoriteP = round(favorite * favoriteR)
    coinP = round(coin * coinR)
    likeP = round(like * likeR)
    point = round(viewP + favoriteP + coinP + likeP)
    
    return viewR, favoriteR, coinR, likeR, viewP, favoriteP, coinP, likeP, point

def plot_graph(view, favorite, coin, like, hascopyright, variable, min_x, max_x):
    x = np.linspace(min_x, max_x, num=max_x - min_x + 1)
    y = np.zeros_like(x)
    
    if variable == "View":
        for i in x:
            _, _, _, _, viewP, favoriteP, coinP, likeP, point = calculate_values(i, favorite, coin, like, hascopyright)
            y[int(i - min_x)] = point
    elif variable == "Favorite":
        for i in x:
            _, _, _, _, viewP, favoriteP, coinP, likeP, point = calculate_values(view, i, coin, like, hascopyright)
            y[int(i - min_x)] = point
    elif variable == "Coin":
        for i in x:
            _, _, _, _, viewP, favoriteP, coinP, likeP, point = calculate_values(view, favorite, i, like, hascopyright)
            y[int(i - min_x)] = point
    elif variable == "Like":
        for i in x:
            _, _, _, _, viewP, favoriteP, coinP, likeP, point = calculate_values(view, favorite, coin, i, hascopyright)
            y[int(i - min_x)] = point
    
    fig, ax = plt.subplots(figsize=(12, 8))
    ax.plot(x, y, label=f'Point vs {variable}', color='dodgerblue', linewidth=2)
    
    # Adjust arrow size dynamically
    def annotate_point(x_value, y_value, label):
        if x_value >= min_x and x_value <= max_x:
            arrow_size = (max_x - min_x) / 1000  # Adjust arrow size based on range
            ax.plot(x_value, y_value, 'ro')
            ax.annotate(f'{label}: {x_value}\nPoint: {y_value}', xy=(x_value, y_value), xytext=(x_value + 0.05 * (max_x - min_x), y_value),
                        arrowprops=dict(facecolor='black', shrink=0.05 * arrow_size), fontsize=12, fontweight='bold')

    if variable == "View" and view >= min_x and view <= max_x:
        _, _, _, _, viewP, favoriteP, coinP, likeP, point = calculate_values(view, favorite, coin, like, hascopyright)
        annotate_point(view, point, variable)
    elif variable == "Favorite" and favorite >= min_x and favorite <= max_x:
        _, _, _, _, viewP, favoriteP, coinP, likeP, point = calculate_values(view, favorite, coin, like, hascopyright)
        annotate_point(favorite, point, variable)
    elif variable == "Coin" and coin >= min_x and coin <= max_x:
        _, _, _, _, viewP, favoriteP, coinP, likeP, point = calculate_values(view, favorite, coin, like, hascopyright)
        annotate_point(coin, point, variable)
    elif variable == "Like" and like >= min_x and like <= max_x:
        _, _, _, _, viewP, favoriteP, coinP, likeP, point = calculate_values(view, favorite, coin, like, hascopyright)
        annotate_point(like, point, variable)
    
    ax.set_xlabel(variable, fontsize=14)
    ax.set_ylabel('Point', fontsize=14)
    ax.set_title(f'Point vs {variable}\nFavorite={favorite}, Coin={coin}, Like={like}', fontsize=16, fontweight='bold')
    ax.legend(fontsize=12)
    ax.grid(True)
    
    return fig

def update_plot(*args):
    try:
        
        view = int(view_entry.get())
        favorite = int(favorite_entry.get())
        coin = int(coin_entry.get())
        like = int(like_entry.get())
        hascopyright = int(copyright_var.get())
        max_value = int(max_value_entry.get())
        variable = variable_var.get()

        if max_value < 500:
            max_value = 500
            max_value_entry.delete(0, tk.END)
            max_value_entry.insert(0, str(max_value))
        elif max_value > 500000:
            max_value = 500000
            max_value_entry.delete(0, tk.END)
            max_value_entry.insert(0, str(max_value))

        fig = plot_graph(view, favorite, coin, like, hascopyright, variable, -100, max_value)
        canvas.figure = fig
        canvas.draw()

        viewR, favoriteR, coinR, likeR, viewP, favoriteP, coinP, likeP, point = calculate_values(view, favorite, coin, like, hascopyright)
        view_value.config(text=f'View: {view}')
        favorite_value.config(text=f'Favorite: {favorite}')
        coin_value.config(text=f'Coin: {coin}')
        like_value.config(text=f'Like: {like}')
        viewR_value.config(text=f'ViewR: {viewR:.2f}')
        favoriteR_value.config(text=f'FavoriteR: {favoriteR:.2f}')
        coinR_value.config(text=f'CoinR: {coinR:.2f}')
        likeR_value.config(text=f'LikeR: {likeR:.2f}')
        viewP_value.config(text=f'ViewP: {viewP}')
        favoriteP_value.config(text=f'FavoriteP: {favoriteP}')
        coinP_value.config(text=f'CoinP: {coinP}')
        likeP_value.config(text=f'LikeP: {likeP}')
        point_value.config(text=f'Point: {point}')
    except ValueError:
        pass

# Create the main window
root = tk.Tk()
root.title("Interactive Plot")

# Create a style for the GUI
style = ttk.Style(root)
style.theme_use('clam')

# Main frame
main_frame = ttk.Frame(root, padding="10")
main_frame.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))

# Create entries and labels
ttk.Label(main_frame, text="View:", font=('Helvetica', 12)).grid(row=0, column=0, sticky=tk.W)
view_entry = ttk.Entry(main_frame, font=('Helvetica', 12))
view_entry.insert(0, "10000")
view_entry.grid(row=0, column=1, sticky=(tk.W, tk.E))

ttk.Label(main_frame, text="Favorite:", font=('Helvetica', 12)).grid(row=1, column=0, sticky=tk.W)
favorite_entry = ttk.Entry(main_frame, font=('Helvetica', 12))
favorite_entry.insert(0, "500")
favorite_entry.grid(row=1, column=1, sticky=(tk.W, tk.E))

ttk.Label(main_frame, text="Coin:", font=('Helvetica', 12)).grid(row=2, column=0, sticky=tk.W)
coin_entry = ttk.Entry(main_frame, font=('Helvetica', 12))
coin_entry.insert(0, "500")
coin_entry.grid(row=2, column=1, sticky=(tk.W, tk.E))

ttk.Label(main_frame, text="Like:", font=('Helvetica', 12)).grid(row=3, column=0, sticky=tk.W)
like_entry = ttk.Entry(main_frame, font=('Helvetica', 12))
like_entry.insert(0, "500")
like_entry.grid(row=3, column=1, sticky=(tk.W, tk.E))


ttk.Label(main_frame, text="Copyright:", font=('Helvetica', 12)).grid(row=4, column=0, sticky=tk.W)
copyright_var = tk.IntVar()
copyright_var.set(1)
copyright_frame = ttk.Frame(main_frame)
copyright_frame.grid(row=4, column=1, sticky=(tk.W, tk.E))
ttk.Radiobutton(copyright_frame, text="1", variable=copyright_var, value=1, command=update_plot).grid(row=0, column=0)
ttk.Radiobutton(copyright_frame, text="2", variable=copyright_var, value=2, command=update_plot).grid(row=0, column=1)

# Create value labels
value_frame = ttk.Frame(main_frame, padding="10")
value_frame.grid(row=5, column=0, columnspan=2, sticky=(tk.W, tk.E))
view_value = ttk.Label(value_frame, text="View: 10000", font=('Helvetica', 12, 'bold'))
view_value.grid(row=0, column=0, sticky=tk.W)
favorite_value = ttk.Label(value_frame, text="Favorite: 500", font=('Helvetica', 12, 'bold'))
favorite_value.grid(row=1, column=0, sticky=tk.W)
coin_value = ttk.Label(value_frame, text="Coin: 500", font=('Helvetica', 12, 'bold'))
coin_value.grid(row=2, column=0, sticky=tk.W)
like_value = ttk.Label(value_frame, text="Like: 500", font=('Helvetica', 12, 'bold'))
like_value.grid(row=3, column=0, sticky=tk.W)
viewR_value = ttk.Label(value_frame, text="ViewR: 0.00", font=('Helvetica', 12, 'bold'))
viewR_value.grid(row=0, column=1, sticky=tk.W)
favoriteR_value = ttk.Label(value_frame, text="FavoriteR: 0.00", font=('Helvetica', 12, 'bold'))
favoriteR_value.grid(row=1, column=1, sticky=tk.W)
coinR_value = ttk.Label(value_frame, text="CoinR: 0.00", font=('Helvetica', 12, 'bold'))
coinR_value.grid(row=2, column=1, sticky=tk.W)
likeR_value = ttk.Label(value_frame, text="LikeR: 0.00", font=('Helvetica', 12, 'bold'))
likeR_value.grid(row=3, column=1, sticky=tk.W)
viewP_value = ttk.Label(value_frame, text="ViewP: 0", font=('Helvetica', 12, 'bold'))
viewP_value.grid(row=0, column=2, sticky=tk.W)
favoriteP_value = ttk.Label(value_frame, text="FavoriteP: 0", font=('Helvetica', 12, 'bold'))
favoriteP_value.grid(row=1, column=2, sticky=tk.W)
coinP_value = ttk.Label(value_frame, text="CoinP: 0", font=('Helvetica', 12, 'bold'))
coinP_value.grid(row=2, column=2, sticky=tk.W)
likeP_value = ttk.Label(value_frame, text="LikeP: 0", font=('Helvetica', 12, 'bold'))
likeP_value.grid(row=3, column=2, sticky=tk.W)
point_value = ttk.Label(value_frame, text="Point: 0", font=('Helvetica', 12, 'bold'))
point_value.grid(row=4, column=0, columnspan=3, sticky=tk.W)

# Create the maximum value entry
ttk.Label(main_frame, text="Maximum Value:", font=('Helvetica', 12)).grid(row=6, column=0, sticky=tk.W)
max_value_entry = ttk.Entry(main_frame, font=('Helvetica', 12))
max_value_entry.insert(0, "100000")
max_value_entry.grid(row=6, column=1, sticky=(tk.W, tk.E))
max_value_entry.bind("<Return>", update_plot)

# Bind the update_plot function to the entries
view_entry.bind("<Return>", update_plot)
favorite_entry.bind("<Return>", update_plot)
coin_entry.bind("<Return>", update_plot)
like_entry.bind("<Return>", update_plot)


# Create a combobox to select the independent variable
ttk.Label(main_frame, text="Independent Variable:", font=('Helvetica', 12)).grid(row=7, column=0, sticky=tk.W)
variable_var = tk.StringVar()
variable_combobox = ttk.Combobox(main_frame, textvariable=variable_var, values=["View", "Favorite", "Coin", "Like"], state="readonly", font=('Helvetica', 12))
variable_combobox.current(0)
variable_combobox.grid(row=7, column=1, sticky=(tk.W, tk.E))
variable_combobox.bind("<<ComboboxSelected>>", update_plot)

# Create a canvas for the plot
fig = plot_graph(int(view_entry.get()), int(favorite_entry.get()), int(coin_entry.get()), int(like_entry.get()), int(copyright_var.get()), variable_var.get(), 0, int(max_value_entry.get()))
canvas = FigureCanvasTkAgg(fig, master=root)
canvas.draw()
canvas.get_tk_widget().grid(row=1, column=0, columnspan=2, pady=10)

# Adjust grid weights
main_frame.columnconfigure(1, weight=1)
root.columnconfigure(0, weight=1)
root.rowconfigure(1, weight=1)

# Run the Tkinter event loop
root.mainloop()
