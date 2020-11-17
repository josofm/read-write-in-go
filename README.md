# read-write-in-go
Read and Write csv in go based on some subject that you want.

Pre requisites:
[Go](https://golang.org/)

To create your own rule, add a sub flag in [calculateNewValue](https://github.com/josofm/read-write-in-go/blob/main/treat_csv.go#L111) function, and write your func

Run:

```
go run treat_csv.go -input=your-file.csv -output=your-output-file.csv -sub=your-subject
```
