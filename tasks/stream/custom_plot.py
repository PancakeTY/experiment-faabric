from invoke import task
import re
import pandas as pd
import matplotlib.pyplot as plt

@task
def state_access(ctx):

    # Create the barplot with log scale to better distinguish the smaller values
    plt.figure(figsize=(10, 6))
    sns.barplot(x='Configuration', y='Lock Duration (μs)', data=df, palette='viridis')
    plt.yscale('log')
    
    # Customize the plot
    plt.xticks(rotation=45, ha='right')
    plt.title('Lock Duration Comparison (Log Scale)')
    plt.ylabel('Lock Duration (μs) [Log Scale]')
    plt.xlabel('Configuration')
    plt.tight_layout()
    
    # Display the plot
    plt.show()
    