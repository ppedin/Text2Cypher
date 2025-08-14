import csv

def calculate_scores(data):
    total_bleu = 0
    total_exact_match = 0
    count = 0
    for row in data:
        try:
            bleu_score = float(row['bleu_score'])
            exact_match_score = float(row['exact_match_score'])
            total_bleu += bleu_score
            total_exact_match += exact_match_score
            count += 1
        except ValueError:
            # Handle cases where scores might not be valid numbers
            continue
    
    if count == 0:
        return 0, 0
    return total_bleu / count, total_exact_match / count


def main():
    file_path = 'evaluation_results.csv'
    all_queries_data = []
    successful_ground_truth_queries_data = []

    with open(file_path, mode='r', newline='') as file:
        reader = csv.DictReader(file)
        for row in reader:
            all_queries_data.append(row)
            if row['ground_truth_result'] != 'ERROR':
                successful_ground_truth_queries_data.append(row)

    # Calculate scores for all queries
    avg_bleu_all, avg_exact_match_all = calculate_scores(all_queries_data)
    print(f"\n--- Overall Evaluation (All Queries) ---")
    print(f"Average BLEU Score: {avg_bleu_all:.4f}")
    print(f"Average Exact Match Score: {avg_exact_match_all:.4f}")

    # Calculate scores for queries with successful ground truth execution
    avg_bleu_successful, avg_exact_match_successful = calculate_scores(successful_ground_truth_queries_data)
    print(f"\n--- Evaluation for Queries with Successful Ground Truth Execution ---")
    print(f"Average BLEU Score: {avg_bleu_successful:.4f}")
    print(f"Average Exact Match Score: {avg_exact_match_successful:.4f}")


if __name__ == "__main__":
    main()
