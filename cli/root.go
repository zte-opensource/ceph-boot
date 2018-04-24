package cli

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"github.com/zte-opensource/ceph-boot/cli/put"
	"github.com/zte-opensource/ceph-boot/cli/ssh"
)

var (
	version bool
)

func init() {
	cobra.OnInitialize(initConfig)

	flags := rootCmd.Flags()
	flags.BoolVar(&version, "version", false, "version")

	rootCmd.AddCommand(put.PutCmd)
	rootCmd.AddCommand(ssh.SshCmd)
}

func initConfig() {
}

var rootCmd = &cobra.Command{
	Use:  "ceph-boot",
	Long: `A fast and reliable utility for Ceph cluster deployment built in Go.`,
	Args: cobra.NoArgs,
	Run: func(cmd *cobra.Command, args []string) {
		if version {
			fmt.Println("1.0")
		}
	},
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
