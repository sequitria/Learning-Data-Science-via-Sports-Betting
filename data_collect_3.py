import requests
import json
import time
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta
from tqdm import tqdm

class WNBADataCollector:
    def __init__(self, api_key):
        self.api_key = api_key
        self.base_url = "http://api.sportradar.us/wnba/trial/v8/en"
        self.sleep_time = 1.5
        self.api_calls = 0
        
    def _make_request(self, endpoint):
        """Make API request with error handling and rate limiting"""
        url = f"{self.base_url}/{endpoint}?api_key={self.api_key}"
        
        try:
            response = requests.get(url)
            self.api_calls += 1
            time.sleep(self.sleep_time)
            
            if response.status_code == 200:
                return response.json()
            else:
                print(f"Error {response.status_code} for {endpoint}")
                return None
                
        except Exception as e:
            print(f"Error fetching {endpoint}: {str(e)}")
            return None
        
    def get_season_games(self, year):
        """Get all games for a season"""
        endpoint = f"games/{year}/REG/schedule.json"
        return self._make_request(endpoint)

    def get_game_statistics(self, game_id):
        """Get detailed game statistics"""
        endpoint = f"games/{game_id}/summary.json"
        return self._make_request(endpoint)

    def get_player_profile(self, player_id):
        """Get player career statistics and profile"""
        endpoint = f"players/{player_id}/profile.json"
        return self._make_request(endpoint)

    def create_directory_structure(self, base_dir, year):
        """Create organized directory structure"""
        # Create base directory
        base_path = Path(base_dir)
        base_path.mkdir(exist_ok=True)
        
        # Create players directory
        players_path = base_path / "Players"
        players_path.mkdir(exist_ok=True)
        
        # Create year directory for games
        year_path = base_path / str(year)
        year_path.mkdir(exist_ok=True)
        
        return base_path, players_path, year_path

    def get_existing_players(self, players_dir):
        """Get list of players that already have profile data"""
        return {f.stem for f in Path(players_dir).glob('*.json')}

    def collect_season_data(self, year, output_dir="wnba_betting_data"):
        """Collect all relevant data for three-point betting analysis"""
        # Create directory structure
        base_path, players_path, year_path = self.create_directory_structure(output_dir, year)
        
        # Get season schedule
        print(f"Downloading {year} schedule...")
        schedule = self.get_season_games(year)
        if schedule:
            with open(year_path / f"schedule_{year}.json", 'w') as f:
                json.dump(schedule, f, indent=4)

        # Get existing players to avoid duplicate requests
        existing_players = self.get_existing_players(players_path)
        all_players = set()

        # Process each game
        games_data = []
        player_game_stats = []
        
        if schedule and 'games' in schedule:
            total_games = len(schedule['games'])
            
            game_pbar = tqdm(schedule['games'], 
                           desc="Processing games", 
                           total=total_games,
                           unit="game")
            
            for game in game_pbar:
                game_id = game['id']
                game_pbar.set_postfix_str(f"Game ID: {game_id}")

                game_stats = self.get_game_statistics(game_id)
                if game_stats:
                    # Save game data in year directory
                    with open(year_path / f"game_{game_id}.json", 'w') as f:
                        json.dump(game_stats, f, indent=4)

                    game_info = {
                        'game_id': game_id,
                        'date': game['scheduled'],
                        'home_team': game_stats['home']['name'],
                        'away_team': game_stats['away']['name'],
                        'venue': game_stats.get('venue', {}).get('name', '')
                    }
                    games_data.append(game_info)
                    for team in ['home', 'away']:
                        if 'players' in game_stats[team]:
                            for player in game_stats[team]['players']:
                                try:
                                    player_id = player.get('id')  # Use get() instead of direct access
                                    if not player_id:  # If id is None or missing
                                        print(f"\nWarning: Missing player ID in game {game_id}")
                                        print(f"Player data: {json.dumps(player, indent=2)}")
                                        continue
										
                                    all_players.add(player_id)
									
                                    stats = player.get('statistics', {})
                                    player_stats = {
										'game_id': game_id,
										'game_date': game['scheduled'],
										'player_id': player_id,
										'player_name': player.get('full_name', 'Unknown'),
										'team': game_stats[team]['name'],
										'opponent': game_stats['away' if team == 'home' else 'home']['name'],
										'home_away': team,
										'starter': player.get('starter', False),
										'minutes': stats.get('minutes', 0),
										'three_points_made': stats.get('three_points_made', 0),
										'three_points_att': stats.get('three_points_att', 0),
										'points': stats.get('points', 0)
									}
                                    player_game_stats.append(player_stats)
                                except Exception as e:
                                    print(f"\nError processing player in game {game_id}, team {team}:")
                                    print(f"Player data: {json.dumps(player, indent=2)}")
                                    print(f"Error: {str(e)}")
                                    continue

        # Save game-related CSVs in year directory
        print("\nSaving game data...")
        games_df = pd.DataFrame(games_data)
        games_df.to_csv(year_path / f"games_{year}.csv", index=False)

        player_stats_df = pd.DataFrame(player_game_stats)
        player_stats_df.to_csv(year_path / f"player_game_stats_{year}.csv", index=False)

        # Collect player profiles (only for new players)
        new_players = all_players - existing_players
        if new_players:
            print(f"\nCollecting {len(new_players)} new player profiles...")
            player_profiles = []
            
            player_pbar = tqdm(list(new_players), 
                             desc="Fetching new player profiles",
                             unit="player")
            
            for player_id in player_pbar:
                player_pbar.set_postfix_str(f"Player ID: {player_id}")
                profile = self.get_player_profile(player_id)
                if profile:
                    # Save player profile in Players directory
                    with open(players_path / f"player_{player_id}.json", 'w') as f:
                        json.dump(profile, f, indent=4)
                    
                    player_info = {
                        'player_id': player_id,
                        'name': profile.get('full_name', ''),
                        'position': profile.get('position', ''),
                        'experience': profile.get('experience', ''),
                        'height': profile.get('height', 0),
                        'weight': profile.get('weight', 0)
                    }
                    player_profiles.append(player_info)

            # Append new profiles to existing profiles CSV
            if player_profiles:
                new_profiles_df = pd.DataFrame(player_profiles)
                profiles_csv_path = base_path / "player_profiles.csv"
                
                if profiles_csv_path.exists():
                    existing_profiles = pd.read_csv(profiles_csv_path)
                    updated_profiles = pd.concat([existing_profiles, new_profiles_df], ignore_index=True)
                else:
                    updated_profiles = new_profiles_df
                    
                updated_profiles.to_csv(profiles_csv_path, index=False)

        print(f"\nData collection complete! Total API calls made: {self.api_calls}")

def main():
    api_key = "PLACE_YOUR_KEY_HEREEEEE"
    collector = WNBADataCollector(api_key)
    
    year = "2021"
    start_time = time.time()
    collector.collect_season_data(year)
    end_time = time.time()
    
    print(f"\nCollection Summary:")
    print(f"Total time: {(end_time - start_time)/60:.2f} minutes")
    print(f"Total API calls: {collector.api_calls}")
    print(f"Remaining API calls: {614 - collector.api_calls}")

if __name__ == "__main__":
    main()