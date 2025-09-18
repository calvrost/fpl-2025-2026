import pandas as pd
import os
from datetime import datetime
import subprocess
import requests

def scrape_data():
    """
    Fetches all player statistics from the official Fantasy Premier League (FPL) API.
    This function returns a pandas DataFrame on success, or None on failure.
    """
    # 1. Define the FPL API endpoint
    api_url = 'https://fantasy.premierleague.com/api/bootstrap-static/'
    print("Fetching data from the Fantasy Premier League API...")

    try:
        # 2. Make the HTTP Request
        response = requests.get(api_url)
        response.raise_for_status()  # Will raise an error for non-2xx status codes
        data = response.json()
        print("Data fetched successfully! Processing player stats...")

        # 3. Extract and map the data
        teams_map = {team['id']: team['name'] for team in data['teams']}
        positions_map = {pos['id']: pos['singular_name_short'] for pos in data['element_types']}
        players_data = data.get('elements', [])

        if not players_data:
            print("Could not find player data in the response.")
            return None

        processed_players = []
        for player in players_data:
            processed_players.append({
                # Custom columns with snake_case naming
                'player_name': f"{player['first_name']} {player['web_name']}",
                'club_name': teams_map.get(player['team'], 'N/A'),
                'position_name': positions_map.get(player['element_type'], 'N/A'),
                
                # Columns using original API field names
                'now_cost': player['now_cost'] / 10.0,
                'total_points': player['total_points'],
                'event_points': player['event_points'],
                'points_per_game': float(player['points_per_game']),
                'selected_by_percent': float(player['selected_by_percent']),
                'goals_scored': player['goals_scored'],
                'assists': player['assists'],
                'minutes': player['minutes'],
                'clean_sheets': player['clean_sheets'],
                'goals_conceded': player['goals_conceded'],
                'own_goals': player['own_goals'],
                'penalties_saved': player['penalties_saved'],
                'penalties_missed': player['penalties_missed'],
                'saves': player['saves'],
                'yellow_cards': player['yellow_cards'],
                'red_cards': player['red_cards'],
                'bonus': player['bonus'],
                'influence': float(player['influence']),
                'creativity': float(player['creativity']),
                'threat': float(player['threat']),
                'ict_index': float(player['ict_index']),
                'form': float(player['form']),
                'dreamteam_count': player['dreamteam_count'],
                'value_form': float(player['value_form']),
                'value_season': float(player['value_season']),
                'transfers_in': player['transfers_in'],
                'transfers_out': player['transfers_out'],
                'transfers_in_event': player['transfers_in_event'],
                'transfers_out_event': player['transfers_out_event'],
                'cost_change_start': player['cost_change_start'] / 10.0,
                'cost_change_start_fall': player['cost_change_start_fall'] / 10.0,
                'cost_change_event': player['cost_change_event'] / 10.0,
                'cost_change_event_fall': player['cost_change_event_fall'] / 10.0,
                'expected_goals': float(player.get('expected_goals', 0)),
                'expected_assists': float(player.get('expected_assists', 0)),
                'expected_goal_involvements': float(player.get('expected_goal_involvements', 0)),
                'expected_goals_conceded': float(player.get('expected_goals_conceded', 0)),
                'starts': player.get('starts', 0),
                'news': player.get('news', ''),
                'influence_rank': player.get('influence_rank'),
                'creativity_rank': player.get('creativity_rank'),
                'threat_rank': player.get('threat_rank'),
                'ict_index_rank': player.get('ict_index_rank'),
            })

        # 4. Create and sort the DataFrame
        df = pd.DataFrame(processed_players)
        df = df.sort_values(by='total_points', ascending=False)

        print(f"Data for {len(df)} players processed successfully.")
        return df

    except requests.exceptions.RequestException as e:
        print(f"A network request error occurred: {e}")
        return None
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return None

def save_to_github(df, filename, repo_path='.'):
    """
    Saves the DataFrame to a CSV file and pushes it to the GitHub repository.
    """
    # Determine the full file path
    file_path = os.path.join(repo_path, filename)
    
    # Save the dataframe to a CSV file
    df.to_csv(file_path, index=False)
    print(f"Data successfully saved to {file_path}")

    # Run Git commands using subprocess
    try:
        # Change to the repository directory if needed
        os.chdir(repo_path)

        # Configure the Git user (required by GitHub Actions)
        subprocess.run(['git', 'config', '--global', 'user.name', 'github-actions[bot]'], check=True)
        subprocess.run(['git', 'config', '--global', 'user.email', 'github-actions[bot]@users.noreply.github.com'], check=True)
        
        # Add the file to the staging area
        print(f"Adding {filename} to Git...")
        subprocess.run(['git', 'add', filename], check=True)
        
        # Create a commit
        commit_message = f"FPL data update: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        print(f"Committing with message: '{commit_message}'")
        # Check if there are changes to commit
        status_result = subprocess.run(['git', 'status', '--porcelain'], capture_output=True, text=True)
        if filename in status_result.stdout:
            subprocess.run(['git', 'commit', '-m', commit_message], check=True)
        else:
            print("No changes to commit.")
            return

        # Push to the remote repository
        print("Pushing to the remote repository...")
        subprocess.run(['git', 'push'], check=True)
        
        print("Data successfully pushed to GitHub.")

    except subprocess.CalledProcessError as e:
        print(f"An error occurred while running a Git command: {e}")
    except FileNotFoundError:
        print("Error: 'git' command not found. Make sure Git is installed.")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")


if __name__ == "__main__":
    # Filename for saving the data
    output_filename = "fpl_player_statistics.csv"
    
    # 1. Run the scraping function
    scraped_df = scrape_data()
    
    # 2. Save and push to GitHub if data was fetched successfully
    if scraped_df is not None and not scraped_df.empty:
        save_to_github(scraped_df, output_filename)
    else:
        print("Failed to fetch FPL data, canceling save process.")
        