hadoop jar "$HOME"/practica3_pablo/jars_pabher_2021/Practica2SINFPabloHernandez.jar input/north.csv output_pabher_2021/consulta"$1" "$1" "$2" "$3"
tput bel
hadoop fs -cat output_pabher_2021/consulta"$1"/part-r-00001 > "$HOME"/practica3_pablo/resultados/consulta"$1".txt;
cat "$HOME"/practica3_pablo/resultados/consulta"$1".txt
