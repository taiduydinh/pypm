import os
import time
import tracemalloc


class AlgoLevelWise:
    all_files = []
    temp = []
    temp1 = []
    DB = {}
    FI = {}
    record_length = set()
    Max = 0
    start_timestamp = 0
    end_time = 0
    current_memory = 0
    MaxMemory = 0
    itemset_count = 0
    transaction_count = 0

    Temp = "mu"
    min_sup = "60p"
    Input_Path = ""
    Output_Path = ""

    @staticmethod
    def memory_usage():
        current, peak = tracemalloc.get_traced_memory()
        AlgoLevelWise.current_memory = current / (1024 * 1024)  # Convert to MB
        AlgoLevelWise.MaxMemory = max(AlgoLevelWise.current_memory, AlgoLevelWise.MaxMemory)

    @staticmethod
    def data_base():
        with open(AlgoLevelWise.Input_Path, 'r') as file:
            for line in file:
                AlgoLevelWise.memory_usage()
                line = line.replace("#SUP", "").strip()
                AlgoLevelWise.transaction_count += 1
                AlgoLevelWise.temp.append(line.split(":"))

    @staticmethod
    def gen_item_space(H, T):
        for record in T:
            AlgoLevelWise.memory_usage()
            key = ""
            for i, item in enumerate(record):
                if i % 2 == 0:
                    key = item.strip()
                else:
                    H[key] = int(item.strip())

    @staticmethod
    def write_closed_txt():
        os.makedirs(AlgoLevelWise.Output_Path, exist_ok=True)  # Ensure output directory exists
        for i in range(1, AlgoLevelWise.Max + 1):
            AlgoLevelWise.memory_usage()
            filename = AlgoLevelWise.Temp + str(i) + ".txt"
            file_path = os.path.join(AlgoLevelWise.Output_Path, filename)
            AlgoLevelWise.read_all_files(file_path)

            with open(file_path, 'w') as file:
                for k, v in AlgoLevelWise.DB.items():
                    if len(k.split()) == i:
                        file.write(f"{k}:{v}\n")

    @staticmethod
    def write_db(S, V, H):
        if S not in H or H[S] < V:
            H[S] = V

    @staticmethod
    def get_length():
        glength = [k.split(" ") for k in AlgoLevelWise.DB.keys()]
        for length_list in glength:
            AlgoLevelWise.memory_usage()
            AlgoLevelWise.record_length.add(len(length_list))

    @staticmethod
    def get_max():
        if AlgoLevelWise.record_length:
            AlgoLevelWise.Max = max(AlgoLevelWise.record_length)

    @staticmethod
    def initial_read_level():
        if AlgoLevelWise.Max > 1:
            filename = AlgoLevelWise.Temp + str(AlgoLevelWise.Max) + ".txt"
            prev_filename = AlgoLevelWise.Temp + str(AlgoLevelWise.Max - 1) + ".txt"

            with open(os.path.join(AlgoLevelWise.Output_Path, filename), 'r') as file:
                for line in file:
                    AlgoLevelWise.memory_usage()
                    AlgoLevelWise.temp.append(line.strip().split(":"))
            AlgoLevelWise.gen_item_space(AlgoLevelWise.DB, AlgoLevelWise.temp)

            with open(os.path.join(AlgoLevelWise.Output_Path, prev_filename), 'r') as file:
                for line in file:
                    AlgoLevelWise.memory_usage()
                    AlgoLevelWise.temp1.append(line.strip().split(":"))
            AlgoLevelWise.gen_item_space(AlgoLevelWise.FI, AlgoLevelWise.temp1)

            AlgoLevelWise.Max -= 1

    @staticmethod
    def read_txt_level():
        filename = AlgoLevelWise.Temp + str(AlgoLevelWise.Max - 1) + ".txt"
        if AlgoLevelWise.Max > 1:
            with open(os.path.join(AlgoLevelWise.Output_Path, filename), 'r') as file:
                for line in file:
                    AlgoLevelWise.memory_usage()
                    AlgoLevelWise.temp.append(line.strip().split(":"))
            AlgoLevelWise.gen_item_space(AlgoLevelWise.FI, AlgoLevelWise.temp)
            AlgoLevelWise.Max -= 1

    @staticmethod
    def initial():
        AlgoLevelWise.DB.clear()
        AlgoLevelWise.DB.update(AlgoLevelWise.FI)
        AlgoLevelWise.FI.clear()
        AlgoLevelWise.temp.clear()
        AlgoLevelWise.temp1.clear()

    @staticmethod
    def gen_subsets():
        for k, v in AlgoLevelWise.DB.items():
            AlgoLevelWise.memory_usage()
            elements = k.split()
            for i in range(len(elements)):
                subset = " ".join(elements[j] for j in range(len(elements)) if j != i)
                subset = subset.strip()
                AlgoLevelWise.write_db(subset, v, AlgoLevelWise.FI)

    @staticmethod
    def write():
        with open(os.path.join(AlgoLevelWise.Output_Path, "output.txt"), 'a') as file:
            for k, v in AlgoLevelWise.DB.items():
                file.write(f"{k} #SUP: {v}\n")

    @staticmethod
    def first_process():
        AlgoLevelWise.memory_usage()
        AlgoLevelWise.data_base()
        AlgoLevelWise.gen_item_space(AlgoLevelWise.DB, AlgoLevelWise.temp)
        AlgoLevelWise.get_length()
        AlgoLevelWise.get_max()
        AlgoLevelWise.write_closed_txt()
        AlgoLevelWise.DB.clear()
        AlgoLevelWise.temp.clear()

    @staticmethod
    def subsets_process():
        AlgoLevelWise.memory_usage()
        AlgoLevelWise.initial_read_level()
        AlgoLevelWise.write()
        AlgoLevelWise.gen_subsets()
        AlgoLevelWise.initial()
        AlgoLevelWise.write()

        while AlgoLevelWise.Max > 1:
            AlgoLevelWise.read_txt_level()
            AlgoLevelWise.gen_subsets()
            AlgoLevelWise.initial()
            AlgoLevelWise.write()

    @staticmethod
    def count():
        with open(os.path.join(AlgoLevelWise.Output_Path, "output.txt"), 'r') as file:
            AlgoLevelWise.itemset_count = sum(1 for _ in file)

    @staticmethod
    def read_all_files(file_path):
        AlgoLevelWise.all_files.append(file_path)

    @staticmethod
    def delete():
        for file_path in AlgoLevelWise.all_files:
            try:
                os.remove(file_path)
            except FileNotFoundError:
                pass

    def run_algorithm(self, input_path, output_path):
        AlgoLevelWise.Input_Path = input_path
        AlgoLevelWise.Output_Path = output_path

        os.makedirs(output_path, exist_ok=True)
        output_file_path = os.path.join(output_path, "output.txt")
        if os.path.exists(output_file_path):
            os.remove(output_file_path)

        tracemalloc.start()
        AlgoLevelWise.start_timestamp = time.perf_counter()  # High-precision start time
        AlgoLevelWise.first_process()

        AlgoLevelWise.memory_usage()
        AlgoLevelWise.subsets_process()

        AlgoLevelWise.end_time = time.perf_counter()  # High-precision end time
        AlgoLevelWise.memory_usage()
        AlgoLevelWise.count()
        AlgoLevelWise.delete()

    def print_stats(self):
        total_time = (AlgoLevelWise.end_time - AlgoLevelWise.start_timestamp) * 1000  # Convert to ms
        print("============= LevelWise - STATS =============")
        print(f" Transactions count from database: {AlgoLevelWise.transaction_count}")
        print(f" Frequent itemsets count: {AlgoLevelWise.itemset_count}")
        print(f" Max memory usage: {AlgoLevelWise.MaxMemory:.2f} MB")
        print(f" Total time ~ {total_time:.2f} ms")
        print("===================================================")


# Main execution
if __name__ == "__main__":
    algo = AlgoLevelWise()
    input_path = "contextMushroom_FCI90.txt"  # Ensure this file exists
    output_path = "levelwise_outputs"  # Directory for output files
    algo.run_algorithm(input_path, output_path)
    algo.print_stats()
