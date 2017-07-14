import os
import csv


def singConv(infile, outfile, exp_id, error):
	if (infile == "" or outfile == ""):
		return
	else:
		with open(infile, 'rb') as indata, open(outfile, 'wb') as outdata:
			indata = csv.reader(indata, delimiter = "\t")
			outdata = csv.writer(outdata, delimiter = "\t")
			attributes = ["ts", "event_type", "SID", "ECID", "session", "game_type", "game_number", "episode_number", "level", "score", "lines_cleared", "completed", "game_duration", "avg_ep_duration", "zoid_sequence", "evt_id", "evt_data1", "evt_data2", "curr_zoid", "next_zoid", "danger_mode", "evt_sequence", "rots", "trans", "path_length", "min_rots", "min_trans", "min_path", "min_rots_diff", "min_trans_diff", "min_path_diff", "u_drops", "s_drops", "prop_u_drops", "drop_lat","initial_lat", "avg_lat", "tetrises_game", "tetrises_level", "delaying", "dropping", "zoid_rot", "zoid_col", "zoid_row", "board_rep", "zoid_rep", "smi_ts", "smi_eyes", "smi_samp_x_l", "smi_samp_x_r", "smi_samp_y_l", "smi_samp_y_r", "smi_diam_x_l", "smi_diam_x_r", "smi_diam_y_l", "smi_diam_y_r", "smi_eye_x_l", "smi_eye_x_r", "smi_eye_y_l", "smi_eye_y_r", "smi_eye_z_l", "smi_eye_z_r", "fix_x", "fix_y", "all_diffs", "all_ht", "all_trans", "cd_1", "cd_2", "cd_3", "cd_4", "cd_5", "cd_6", "cd_7", "cd_8","cd_9", "cleared", "col_trans", "column_9", "cuml_cleared", "cuml_eroded", "cuml_wells", "d_all_ht", "d_max_ht", "d_mean_ht", "d_pits", "deep_wells", "eroded_cells", "full_cells", "jaggedness", "landing_height", "lumped_pits", "matches", "max_diffs", "max_ht", "max_ht_diff", "max_well", "mean_ht", "mean_pit_depth", "min_ht", "min_ht_diff", "move_score", "nine_filled", "pattern_div", "pit_depth", "pit_rows", "pits", "row_trans", "tetris", "tetris_progress", "weighted_cells", "wells", "exp_id", 'game_seed', 'height', 'avgheight', 'roughness', 'ridge_len', 'ridge_len_sqr', 'tetris_available', 'filled_rows_covered', 'tetrises_covered', 'good_pos_curr', 'good_pos_next', 'good_pos_any']
			# if the unknown column is evt column
			evt = 0
			outdata.writerow(attributes)
			for row in indata:
				error = 0
				wrow = ["\N"]*len(attributes)
				for i, value in enumerate(row):
					index = -1
					if (value != ""):
						if (value[0] == ':'):
							if (value[1:] in attributes):
								index = attributes.index(value[1:])
								wrow[index] = row[i+1]
							else:
								if (evt == 0):
									# then the unknown : is belong evt_id
									wrow[attributes.index("evt_id")] = value[1:]
									wrow[attributes.index("evt_data1")] = row[i+1]
									wrow[attributes.index("evt_data2")] = "setup"
								else:
									#write to error
									f.write(str(row))
									f.write('\n')
									if (value[1:] not in err):
										err.append(value[1:])
									error = 1
						else:
							continue
				wrow[-1] = exp_id
				if (error == 0):
					outdata.writerow(wrow)
				if ("Start" in wrow):
					evt = 1



if __name__ == "__main__":
	f = open("error3.txt", "wb")
	singConv("/Volumes/research/CogWorksLab/cogback/Projects/Tetris/Lab_Experiments/2013_Rational Tetris/Data/d3/ctrl_train_d3/107_2013_7_10_16-48-33/107_2013_7_10_16-48-33.tsv", "test4.tsv",1, f)
	f.close()