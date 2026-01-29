"""
Visualization Module - ETL Pipeline
"""

import matplotlib.pyplot as plt


def create_scatter_plot(df):
    """
    Create scatter plot: sepal_length vs petal_length
    """
    plt.figure(figsize=(10, 6))

    plt.scatter(df['sepal_length'], df['petal_length'], alpha=0.6, color='blue')

    plt.xlabel('Sepallængde (sepal_length)', fontsize=12)
    plt.ylabel('Kronbladslængde (petal_length)', fontsize=12)
    plt.title('Scatter Plot: Sepal Length vs Petal Length (Iris-setosa)', fontsize=14, fontweight='bold')

    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.show()

    print("[OK] Scatter plot displayed")


def create_histogram(df):
    """
    Create histogram of petal_width distribution
    """
    plt.figure(figsize=(10, 6))

    plt.hist(df['petal_width'], bins=10, color='steelblue', edgecolor='black', alpha=0.7)

    plt.xlabel('Kronbladsbredde (petal_width)', fontsize=12)
    plt.ylabel('Frekvens', fontsize=12)
    plt.title('Histogram: Petal Width (Iris-setosa)', fontsize=14, fontweight='bold')

    plt.grid(True, alpha=0.3, axis='y')
    plt.tight_layout()
    plt.show()

    print("[OK] Histogram displayed")


def create_boxplots(df):
    """
    Create 2x2 boxplot layout for all measurements
    """
    fig, axes = plt.subplots(2, 2, figsize=(12, 10))
    fig.suptitle('Boxplots af alle numeriske Iris-setosa målinger', fontsize=16, fontweight='bold')

    axes[0, 0].boxplot(df['sepal_length'])
    axes[0, 0].set_ylabel('Værdi', fontsize=10)
    axes[0, 0].set_xlabel('sepal_length', fontsize=10)
    axes[0, 0].set_title('Sepallængde', fontsize=12)
    axes[0, 0].grid(True, alpha=0.3)

    axes[0, 1].boxplot(df['sepal_width'])
    axes[0, 1].set_ylabel('Værdi', fontsize=10)
    axes[0, 1].set_xlabel('sepal_width', fontsize=10)
    axes[0, 1].set_title('Sepalbredde', fontsize=12)
    axes[0, 1].grid(True, alpha=0.3)

    axes[1, 0].boxplot(df['petal_length'])
    axes[1, 0].set_ylabel('Værdi', fontsize=10)
    axes[1, 0].set_xlabel('petal_length', fontsize=10)
    axes[1, 0].set_title('Kronbladslængde', fontsize=12)
    axes[1, 0].grid(True, alpha=0.3)

    axes[1, 1].boxplot(df['petal_width'])
    axes[1, 1].set_ylabel('Værdi', fontsize=10)
    axes[1, 1].set_xlabel('petal_width', fontsize=10)
    axes[1, 1].set_title('Kronbladsbredde', fontsize=12)
    axes[1, 1].grid(True, alpha=0.3)

    plt.tight_layout()
    plt.show()

    print("[OK] Boxplots displayed")
