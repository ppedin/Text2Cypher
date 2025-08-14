import evaluate

def bleu_score(predicted_query, ground_truth_query):
    if type(predicted_query) != str:
        return 0.0
    google_bleu = evaluate.load("google_bleu")
    reference = [ground_truth_query.strip().lower().split()]
    candidate = predicted_query.strip().lower().split()
    try:
        bleu_output = google_bleu.compute(predictions=[" ".join(candidate)], references=[[" ".join(reference[0])]])
        print(f"BLEU output: {bleu_output}")
        return bleu_output["google_bleu"]
    except Exception as e:
        print(f"Error computing BLEU score: {e}")
        return 0.0