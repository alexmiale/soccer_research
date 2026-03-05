import numpy as np
# conda install conda-forge::mplsoccer
import mplsoccer as mpl
import matplotlib.pyplot as plt
from pymongo import MongoClient
import json

class mongo_api:
    """
    initialize client in Mongo and add one json file to collection
    file_path:str -> path to file, either relative or complete
    """
    def __init__(self, file_path: str):
        self.client = MongoClient() # setup client
        self.db = self.client["ds4300-assignment3"] # define db
        self.collection = self.db["soccer"] # define connection
        self.collection.drop() # drop connection to avoid overloading inserts

        with open(file_path) as f: # read json file
            file_data = json.load(f)

        self.collection.insert_many(file_data) # insert into collection


    # method to close client outside of the class
    def close_client(self):
        self.client.close()


    def pitch_map(self, player_name: str, event_type: str, save_path:None, half=None):    
        """
        Creates a map in mplsoccer visualizing the given events of the given player for the given half
        player_name: str -> the name of the player
        event_type: str -> the event the user wants to visualize
        save_path: str -> path to save fig to
        half: None or int -> which half the user wants to visualize, 1 for first half, 2 for second half, None for both
        """    
        event_type_lower = event_type.lower() # get lowercase of event_type for use in query
        start_and_end = ["Pass", "Carry", "Shot"] # list of events that have end coordinates, use arrows for visualization

        player_match = { "$match": { "player.name": player_name } } # player name query
        event_match = { "$match": { "type.name": event_type } } # event query

        pipeline = [player_match, event_match] # begin pipeline

        if half is not None: # if half is passed in 
            period = { "$match": { "period" : half }} # query
            pipeline.append(period) # add to pipeline

        project = { "$project": { "_id":0, "location":1, event_type_lower + ".end_location": 1} } # return only attributes we want
        pipeline.append(project) # add to pipeline

        coordinates = list(self.collection.aggregate(pipeline)) # query, wrap in list to preserve type
        
        if event_type in start_and_end: # if end location
            start = list(map(lambda event: event["location"], coordinates)) # get all start coordinates
            end = list(map(lambda event: event[event_type_lower]["end_location"][:2], coordinates)) # get all end coordinates, take first two values because shot has unnecessary z axis
            x1, y1 = np.array(start).T # Transpose to individual x and y lists
            x2, y2 = np.array(end).T

            pitch = mpl.Pitch() # draw the pitch
            if event_type == "Shot": # draw vertical pitch to see shots better
                pitch = mpl.VerticalPitch(half=True, goal_type='box', pad_bottom=-20)

            fig, ax = pitch.draw()
            ax.set_title(f"{event_type} map for {player_name} in Period {half}") # title

            p = pitch.arrows(x1, y1, x2, y2, alpha=0.4, color="blue",
                             headaxislength=3, headlength=3, headwidth=4, width=2, ax=ax) # draw arrows
            if save_path is not None:
                plt.savefig(save_path)
            else:
                plt.show()
        
            return 0 # success
    
        elif event_type not in start_and_end: # scatter plot
            values = list(map(lambda event: event["location"], coordinates)) # get all coordinates
            x1, y1 = np.array(values).T # Transpose to individual x and y lists

            pitch = mpl.Pitch() # draw pitch
            fig, ax = pitch.draw()
            p2 = pitch.scatter(x1, y1, alpha=0.7, color="blue", ax=ax) # plot points
            ax.set_title(f"{event_type} map for {player_name} in Period {half}") # title
            plt.savefig(save_path)
            return 0 # success

        else: return -1 # something went wrong


    def agg_stats(self, stats: list[str], team=None, half=None):
        """
        Gets aggregate stats for either team in either half or whole game
        stats: list[str] -> the aggregate stats to display
        team: None or str -> which team to get stats for or None for both
        half: None or int -> half 1, half 2, or whole game
        return dict of teams or list of dict from one team to be used in plotting functions
        """
        pipeline = []
        if team is None:
            city = self.agg_stats(stats, "Manchester City WFC", half)
            chelsea  = self.agg_stats(stats, "Chelsea FCW", half)
            return {"Manchester City WFC": city, "Chelsea FCW": chelsea}
        else: # team is given
            team_match = { "$match": { "team.name": team}}
            pipeline.append(team_match)
        
        stats_match = { "$match": { "type.name": { "$in": stats }}} # get stats to aggregate
        pipeline.append(stats_match) 

        if half is not None: # if half is passed in 
            period = { "$match": { "period" : half }} # query
            pipeline.append(period) # add to pipeline

        group = { "$group": { "_id": "$type.name", "count": { "$sum":1 }}} # group stats by type 
        pipeline.append(group)

        agg = list(self.collection.aggregate(pipeline)) # query, wrap in list to preserve type

        return agg
    
    
    def side_by_side_plot(self, stats, half=None, save_path=None):
        """
        side by side bar plot for aggregate stats of both teams
        stats: list of dict to plot
        half: which half was used or none for whole game
        save_path: path to save figure
        """
        labels = set() # initialize set
        for team in stats.values(): # get labels from both teams
            for each in team:
                labels.add(each["_id"]) # wont add doubles

        x1 = np.zeros(len(labels)) # init x1
        x2 = np.zeros(len(labels)) # init x2
        values = list(stats.values())

        first_id = list(map(lambda counts: counts["_id"], values[0])) # get ids of first team
        first_count = list(map(lambda counts: counts["count"], values[0])) # get counts of ids

        second_id = list(map(lambda counts: counts["_id"], values[1])) # get ids of second team
        second_count = list(map(lambda counts: counts["count"], values[1])) # get counts of ids

        index_to_add_1 = []
        index_to_add_2 = []

        labels_list = list(labels) # change to list for indexing
        for id in first_id: # for each id
            index_to_add_1.append(labels_list.index(id)) # find index of that id in label

        for id in second_id: # for each id
            index_to_add_2.append(labels_list.index(id)) # find index of that id in label

        
        # add counts at correct indices
        np.add.at(x1, index_to_add_1, first_count)
        np.add.at(x2, index_to_add_2, second_count)

        w = 0.4
        x = np.arange(len(labels_list))

        fig, ax = plt.subplots()
        plt.bar(x - w/2, x1, w, label = "Manchester City WFC") # teams hardcoded into agg_stats function
        plt.bar(x + w/2, x2, w, label = "Chelsea FCW")
        plt.xticks(x, labels_list, rotation=30)
        for tick in ax.xaxis.get_majorticklabels(): # make sure each name is readable
            tick.set_horizontalalignment("right")
        fig.tight_layout() # make sure each name is in figure
        plt.legend()
        plt.ylabel("Counts")
        plt.title(f"Side by Side Bar Graph Comparing Aggregate Stats in Period {half}")

        if save_path is not None:
            plt.savefig(save_path)
        else:
            plt.show()
        
        return 0 # success
    
    
    def bar_plot(self, stats: list[dict], team:str, title: str, half=None, save_path=None):
        """
        bar plot for agg stats of one team if one team is input to agg_stats
        stats: list of dict to plot
        team: the team name for this plot
        half: which half was used or None for whole game
        save_path: path to save figure or None
        """
        x = []
        y = []

        for each in stats:
            x.append(each["_id"])
            y.append(each["count"])

        plt.bar(x, y)
        plt.title(f"Aggregate Stats for {team} in Period {half}")
        plt.legend()
        
        if save_path is not None:
            plt.savefig(save_path)
        else:
            plt.show()
    
        return 0 # success
    
    def agg_player(self, stat, n, sort, half=None, team=None, save_path=None):
        """
        Returns the top or bottom n number of players based on the desired stat, which half, or which team
        stat: str -> the stat to aggregate over
        n: int -> the number of players to return
        sort: int -> to sort in ascending or descening order
        half: int -> 1st half, 2nd half, or whole game
        team: str-> Manchester City, Chelsea, or both teams
        save_path: str-> path to save at or None to just print
        """
        pipeline = [] # pipeline init

        stat_match = { "$match": { "type.name": stat } } # stat query
        pipeline.append(stat_match)

        if half is not None: # if specific half is desired
            period = { "$match": { "period": half }} # query
            pipeline.append(period) # add to pipeline
        
        if team is not None:
            team_match = { "$match": { "team.name": team }} # get team
            pipeline.append(team_match)

        group = { "$group": { "_id": "$player.name", "count": { "$sum": 1 }}} # group by name
        sort = { "$sort": {"count": sort }} # sort clause
        limit = { "$limit": n } # limit clause
        filter = [group, sort, limit]
        for item in filter:
            pipeline.append(item)


        agg = list(self.collection.aggregate(pipeline)) # query, wrap in list to preserve type

        x = []
        y = []

        for each in agg:
            x.append(each["_id"])
            y.append(each["count"])

        sort_string = ""
        if sort == 1: # if 1, ascending list, worst players
            sort_string= "Bottom"
        else:
            sort_string="Top"

        fig, ax = plt.subplots()
        plt.bar(x, y)
        plt.title(f"Aggregate {stat} stat for {sort_string} {n} players")
        plt.legend()
        plt.xticks(rotation=30)
        for tick in ax.xaxis.get_majorticklabels(): # make sure each name is readable
            tick.set_horizontalalignment("right")
        fig.tight_layout() # make sure each name is in figure
        
        if save_path is not None:
            plt.savefig(save_path)
        else:
            plt.show()
    
        return 0


def main():
    mongo = mongo_api("7298.json")

    # demonstration of player map
    player_map = mongo.pitch_map("Millie Bright", "Pass", "millie_bright_pass.png")

    # demonstration of team agg stat
    team_stat = ["Shot", "Pass", "Dribbled Past", "Block", "Clearance", "Interception", "Carry", "Offside"]
    team_stats1 = mongo.agg_stats(team_stat, half=1)
    mongo.side_by_side_plot(team_stats1, half=1, save_path="team_half1.png")
    team_stats2 = mongo.agg_stats(team_stat, half=2)
    mongo.side_by_side_plot(team_stats2, half=2, save_path="team_half2.png")

    # demonstration of player agg stat
    player_stat = mongo.agg_player("Shot", 10, -1, save_path="10_most_shots.png")

    mongo.close_client()

if __name__=="__main__":
    main()