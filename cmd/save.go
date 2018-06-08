package cmd

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/qri-io/dataset"
	"github.com/qri-io/dataset/dsutil"
	"github.com/qri-io/qri/core"
	"github.com/qri-io/qri/repo"
	"github.com/spf13/cobra"
	"io/ioutil"
)

var (
	saveFilePath       string
	saveDataPath       string
	saveTitle          string
	saveMessage        string
	savePassive        bool
	saveRescursive     bool
	saveShowValidation bool
	saveNoRegistry     bool
	saveSecrets        []string
)

// saveCmd represents the save command
var saveCmd = &cobra.Command{
	Use:     "save",
	Aliases: []string{"update", "commit"},
	Short:   "save changes to a dataset",
	Long: `
Save is how you change a dataset, updating one or more of data, metadata, and 
structure. You can also update your data via url. Every time you run save, 
an entry is added to your dataset’s log (which you can see by running “qri log 
[ref]”). Every time you save, you can provide a message about what you changed 
and why. If you don’t provide a message 
qri will automatically generate one for you.

Currently you can only save changes to datasets that you control. Tools for 
collaboration are in the works. Sit tight sportsfans.`,
	Example: `  save updated data to dataset annual_pop:
  $ qri --data /path/to/data.csv me/annual_pop

  save updated dataset (no data) to annual_pop:
  $ qri --file /path/to/dataset.yaml me/annual_pop`,
	Annotations: map[string]string{
		"group": "dataset",
	},
	PreRun: func(cmd *cobra.Command, args []string) {
		loadConfig()
	},
	Run: func(cmd *cobra.Command, args []string) {
		if len(args) < 1 && saveFilePath == "" {
			ErrExit(fmt.Errorf("please provide the name of an existing dataset to save updates to, or specify a dataset --file with name and peername"))
		}

		var arg string
		if len(args) == 1 {
			arg = args[0]
		}
		ref, err := repo.ParseDatasetRef(arg)
		if err != nil && saveFilePath == "" {
			ErrExit(err)
		}

		dsp := &dataset.DatasetPod{}
		if saveFilePath != "" {
			f, err := os.Open(saveFilePath)
			ExitIfErr(err)

			switch strings.ToLower(filepath.Ext(saveFilePath)) {
			case ".yaml", ".yml":
				data, err := ioutil.ReadAll(f)
				ExitIfErr(err)
				err = dsutil.UnmarshalYAMLDatasetPod(data, dsp)
				ExitIfErr(err)
			case ".json":
				err = json.NewDecoder(f).Decode(dsp)
				ExitIfErr(err)
			}
		}

		if ref.Name != "" {
			dsp.Name = ref.Name
		}
		if ref.Peername != "" {
			dsp.Peername = ref.Peername
		} else if dsp.Peername == "" {
			dsp.Peername = "me"
		}

		if (saveTitle != "" || saveMessage != "") && dsp.Commit == nil {
			dsp.Commit = &dataset.CommitPod{}
		}
		if saveTitle != "" {
			dsp.Commit.Title = saveTitle
		}
		if saveMessage != "" {
			dsp.Commit.Message = saveMessage
		}

		if saveDataPath != "" {
			dsp.DataPath = saveDataPath
		}

		if dsp.Transform != nil && saveSecrets != nil {
			if !confirm(`
Warning: You are providing secrets to a dataset transformation.
Never provide secrets to a transformation you do not trust.
continue?`, true) {
				return
			}

			dsp.Transform.Secrets, err = parseSecrets(addDsSecrets...)
			ExitIfErr(err)
		}

		p := &core.SaveParams{
			Dataset: dsp,
			Private: false,
			Publish: !saveNoRegistry,
		}

		req, err := datasetRequests(false)
		ExitIfErr(err)

		res := &repo.DatasetRef{}
		err = req.Save(p, res)
		ExitIfErr(err)

		printSuccess("dataset saved: %s", res)
		if res.Dataset.Structure.ErrCount > 0 {
			printWarning(fmt.Sprintf("this dataset has %d validation errors", res.Dataset.Structure.ErrCount))

			// TODO - restore. This should read from the created dataset instead of input data
			// if saveShowValidation {
			// 	printWarning("Validation Error Detail:")
			// 	data, err := ioutil.ReadAll(dataFile)
			// 	ExitIfErr(err)
			// 	ds, err := res.DecodeDataset()
			// 	ExitIfErr(err)
			// 	errorList, err := ds.Structure.Schema.ValidateBytes(data)
			// 	ExitIfErr(err)
			// 	for i, validationErr := range errorList {
			// 		printWarning(fmt.Sprintf("\t%d. %s", i+1, validationErr.Error()))
			// 	}
			// }
		}
	},
}

func init() {
	saveCmd.Flags().StringVarP(&saveFilePath, "file", "f", "", "dataset data file (yaml or json)")
	saveCmd.Flags().StringVarP(&saveTitle, "title", "t", "", "title of commit message for save")
	saveCmd.Flags().StringVarP(&saveMessage, "message", "m", "", "commit message for save")
	saveCmd.Flags().StringVarP(&saveDataPath, "data", "", "", "path to file or url to initialize from")
	saveCmd.Flags().BoolVarP(&saveShowValidation, "show-validation", "s", false, "display a list of validation errors upon adding")
	saveCmd.Flags().StringSliceVar(&saveSecrets, "secrets", nil, "transform secrets as comma separated key,value,key,value,... sequence")
	saveCmd.Flags().BoolVarP(&saveNoRegistry, "no-registry", "n", false, "don't publish this dataset to the registry")
	RootCmd.AddCommand(saveCmd)
}
