from airflow.decorators import dag, task
from datetime import datetime

from dotenv import load_dotenv
from kaggle.api.kaggle_api_extended import KaggleApi
import pandas as pd
import os, json

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
# from textwrap import wrap

from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas

@dag(
    dag_id='coffee_data_taskflow',
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['assignment3']
)
def assignment_taskflow():
    @task
    def extract_coffee_data():
        # First store Kaggle token kaggle.json at ~/.kaggle/kaggle.json
        api = KaggleApi()
        api.authenticate()
        # Download and store csv file into folder 'outputs'
        api.dataset_download_files('sujaykapadnis/lets-do-some-coffee-tasting', 'outputs', unzip=True)
        # Read csv
        df = pd.read_csv('outputs/GACTT_RESULTS_ANONYMIZED_v2.csv')
        return 'outputs/GACTT_RESULTS_ANONYMIZED_v2.csv'

    @task
    def transform_coffee_data(csv_file_path):
        df = pd.read_csv(csv_file_path)
        # Filter to focus on survey respondents who typically drink coffee at a cafe
        df_filtered = df[df['Where do you typically drink coffee? (At a cafe)'] == True].copy()
        
        coffee_stats = {}
        
        # Education
        education_dict = df_filtered['Education Level'].fillna('NA').value_counts().to_dict()
        coffee_stats['Education'] = education_dict
        
        # Employment Status
        employment_status_dict = df_filtered['Employment Status'].fillna('NA').value_counts().to_dict()
        coffee_stats['Employment Status'] = employment_status_dict
        
        # Spending Habits
        spending_habits_dict = df_filtered['In total, much money do you typically spend on coffee in a month?'].fillna('NA').value_counts().to_dict()
        coffee_stats['Spending Habits'] = spending_habits_dict
        
        # Preferences on Coffee Strength
        value_mapping = {
            'Somewhat strong': 'Strong',
            'Very strong': 'Strong',
            'Somewhat light': 'Weak',
        }
        df_filtered['How strong do you like your coffee? (Grouped)'] = df_filtered['How strong do you like your coffee?'].fillna('NA').replace(value_mapping)
        preferences_on_coffee_strength_dict = df_filtered['How strong do you like your coffee? (Grouped)'].value_counts().to_dict()
        coffee_stats['Preferences on Coffee Strength'] = preferences_on_coffee_strength_dict
        
        # Drink Preferences
        drink_preferences_dict = df_filtered['What is your favorite coffee drink?'].fillna('NA').value_counts().nlargest(5).to_dict()
        coffee_stats['Drink Preferences'] = drink_preferences_dict
        
        # Setting up file directory
        filename = 'outputs/coffee_stats.json'
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        
        # Serialize coffee_stats into a JSON file
        with open(filename, 'w') as file:
            json.dump(coffee_stats, file, indent=4)
        
        return filename
    
    @task
    def generate_coffee_report(json_file_path):
        with open(json_file_path) as file:
            coffee_stats = json.load(file)
            
        # Setting up file directory
        plots_path = 'outputs'
        os.makedirs(plots_path, exist_ok=True)
        image_path_dict = {}
        
        # Education
        education_dict = coffee_stats['Education']
        labels = []
        sizes = []
        for education_level, count in education_dict.items():
            labels.append(education_level)
            sizes.append(count)
        # labels = [ '\n'.join(wrap(l, 20)) for l in labels ]
        plt.pie(sizes, labels=labels)
        plt.title(label='Education')
        education_image_path = plots_path + '/education.png'
        plt.savefig(education_image_path, bbox_inches='tight')
        plt.close()
        image_path_dict['Education'] = education_image_path
        
        # Employment Status
        employment_status_dict = coffee_stats['Employment Status']
        labels = []
        sizes = []
        for employment_status, count in employment_status_dict.items():
            labels.append(employment_status)
            sizes.append(count)
        plt.bar(range(len(employment_status_dict)), sizes, tick_label=labels)
        plt.xticks(rotation=45, ha='right')
        plt.title(label='Employment Status')
        employment_status_image_path = plots_path + '/employment_status.png'
        plt.savefig(employment_status_image_path, bbox_inches='tight')
        plt.close()
        image_path_dict['Employment Status'] = employment_status_image_path
        
        # Spending Habits
        spending_habits_dict = coffee_stats['Spending Habits']
        labels = []
        sizes = []
        for spending_range, count in spending_habits_dict.items():
            labels.append(spending_range)
            sizes.append(count)
        plt.bar(range(len(spending_habits_dict)), sizes, tick_label=labels)
        plt.xticks(rotation=45, ha='right')
        plt.title(label='Spending Habits')
        spending_habits_image_path = plots_path + '/spending_habits.png'
        plt.savefig(spending_habits_image_path, bbox_inches='tight')
        plt.close()
        image_path_dict['Spending Habits'] = spending_habits_image_path
        
        # Preferences on Coffee Strength
        preferences_on_coffee_strength_dict = coffee_stats['Preferences on Coffee Strength']
        labels = []
        sizes = []
        for coffee_strength, count in preferences_on_coffee_strength_dict.items():
            labels.append(coffee_strength)
            sizes.append(count)
        plt.pie(sizes, labels=labels)
        plt.title(label='Preferences on Coffee Strength')
        preferences_on_coffee_strength_image_path = plots_path + '/preferences_on_coffee_strength.png'
        plt.savefig(preferences_on_coffee_strength_image_path, bbox_inches='tight')
        plt.close()
        image_path_dict['Preferences on Coffee Strength'] = preferences_on_coffee_strength_image_path
        
        # Drink Preferences
        drink_preferences_dict = coffee_stats['Drink Preferences']
        labels = []
        sizes = []
        for drink, count in drink_preferences_dict.items():
            labels.append(drink)
            sizes.append(count)
        plt.bar(range(len(drink_preferences_dict)), sizes, tick_label=labels)
        plt.xticks(rotation=45, ha='right')
        plt.title(label='Drink Preferences')
        drink_preferences_image_path = plots_path + '/drink_preferences.png'
        plt.savefig(plots_path + '/drink_preferences.png', bbox_inches='tight')
        plt.close()
        image_path_dict['Drink Preferences'] = drink_preferences_image_path
        
        # Setting up file directory
        pdfs_path = 'outputs'
        os.makedirs(pdfs_path, exist_ok=True)
        
        # Generating PDF
        filename = pdfs_path + '/coffee_report.pdf'
        c = canvas.Canvas(filename, pagesize=letter)
        width, height = letter # get the dimensions of the page
        
        # Set up the font and size for the text
        c.setFont("Times-Roman", 12)
        ONE_INCH = 96
        line_height = 12
        image_height = 3 * ONE_INCH
        margin = ONE_INCH
        current_height = height - margin
        
        # Loop through each category and its details
        for category, details in coffee_stats.items():
            # Check if there's enough space for the next section
            if current_height < (margin * 2 + line_height):
                c.showPage()
                current_height = height - margin
                
            # Add an plot for the category
            image_path = image_path_dict[category]
            c.drawImage(image_path, margin, current_height - image_height, width=300, height=image_height)
            current_height -= (image_height + margin)

        # Finalize and save the PDF
        c.save()
        
    coffee_data = extract_coffee_data()
    json_filename = transform_coffee_data(coffee_data)
    generate_coffee_report(json_filename)
        
assignment_dag = assignment_taskflow()