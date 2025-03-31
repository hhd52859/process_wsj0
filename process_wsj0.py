import os
import argparse
from pathlib import Path
import subprocess
from tqdm import tqdm
import tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed

def extract_subsets_and_subfolders(file_path):
    subsets = set()  # Use set for deduplication
    subfolders = set()  # Store subfolders

    with open(file_path, 'r') as f:
        for line in f:
            # Each line format: <md5_hash> <size> <timestamp> <path_to_file>
            parts = line.split()
            if len(parts) != 4:
                continue  # Skip invalid lines
            
            file_path = parts[3]  # Get file path (should be the last field)
            
            path_parts = file_path.split('/')
            
            if len(path_parts) < 4:
                continue  # Skip invalid paths

            # Extract subset part (e.g., si_dt_20 or sd_dt_20)
            if path_parts[-3].startswith("si_") or path_parts[-3].startswith("sd_"):
                subsets.add(path_parts[-3])
                # Extract subfolder (e.g., 40n)
                if len(path_parts) >= 5:
                    subfolders.add(path_parts[-2])

    return subsets, subfolders

def find_wv1_files(root):
    return list(Path(root).rglob("*.wv1")) + list(Path(root).rglob("*.sph"))

def convert_to_wav(wv1_path, wav_path, sph2pipe_path="sph2pipe"):
    """Convert .wv1 to 16kHz .wav"""
    cmd = [sph2pipe_path, "-f", "wav", str(wv1_path)]
    with open(wav_path, "wb") as f:
        subprocess.run(cmd, stdout=f, check=True)

def resample_audio(wav_in_path, wav_out_path, target_sample_rate):
    """Resample .wav to specified sample rate and save"""
    cmd = ["sox", str(wav_in_path), "-r", str(target_sample_rate), str(wav_out_path)]
    subprocess.run(cmd, check=True)

def extract_set_dir(wv1_path, subsets, subfolders):
    # Find matching subset and subfolder
    for part in wv1_path.parts:
        if part in subsets:
            subset = part
            break
    else:
        raise ValueError(f"Could not find subset for {wv1_path}")

    # Generate target folder structure
    subfolder = None
    for part in wv1_path.parts:
        if part in subfolders:
            subfolder = part
            break
    else:
        subfolder = "default"  # Use default folder if not found

    return subset, subfolder

def process_file(wv1_file, subsets, subfolders, output_root, sph2pipe_path, target_sample_rate):
    """Process each .wv1 file: convert to .wav and resample to specified rate"""
    try:
        subset, subfolder = extract_set_dir(wv1_file, subsets, subfolders)
        utt_id = wv1_file.stem
        out_path = Path(output_root) / subset / subfolder / f"{utt_id}.wav"
        os.makedirs(out_path.parent, exist_ok=True)

        with tempfile.NamedTemporaryFile(suffix=".wav") as tmp_wav:
            convert_to_wav(wv1_file, tmp_wav.name, sph2pipe_path)
            resample_audio(tmp_wav.name, out_path, target_sample_rate)

        return f"{wv1_file} converted successfully, saved to {out_path}"

    except Exception as e:
        return f" {wv1_file} conversion failed: {e}"

def main(args):
    wv1_files = find_wv1_files(args.input_root)
    print(f"Found {len(wv1_files)} .wv1/.sph files")

    subsets, subfolders = extract_subsets_and_subfolders(os.path.join(args.input_root, "file.tbl"))
    print(f"Extracted subsets: {subsets}")
    print(f"Extracted subfolders: {subfolders}")

    # Create thread pool and start parallel processing
    with ThreadPoolExecutor(max_workers=args.num_threads) as executor:
        futures = [executor.submit(process_file, wv1_file, subsets, subfolders, args.output_root, args.sph2pipe_path, args.sample_rate)
                   for wv1_file in wv1_files]
        
        for future in tqdm(as_completed(futures), total=len(futures), desc="Processing"):
            print(future.result())

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_root", required=True, help="Path to original WSJ0 data")
    parser.add_argument("--output_root", required=True, help="Target path for output .wav files")
    parser.add_argument("--sph2pipe_path", default="sph2pipe", help="Path to sph2pipe command")
    parser.add_argument("--num_threads", type=int, default=24, help="Number of parallel threads")
    parser.add_argument("--sample_rate", type=int, default=8000, help="Target sample rate for output files (default: 8000)")
    args = parser.parse_args()
    main(args)
