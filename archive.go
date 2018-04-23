package main

import (
	"archive/tar"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"syscall"

	"github.com/zte-opensource/ceph-boot/hierr"
	"github.com/zte-opensource/ceph-boot/log"
	"github.com/zte-opensource/ceph-boot/writer"
)

type file struct {
	path string
	size int
}

func startArchiveReceivers(
	cluster *Cluster,
	rootDir string,
	sudo bool,
	serial bool,
) error {
	command := []string{
		"mkdir", "-p", rootDir, "&&", "tar", "--directory", rootDir, "-x",
	}

	if log.Conf.Verbose >= log.VerbosityDebug {
		command = append(command, `--verbose`)
	}

	raw := &RawCommand{
		command: command,
		shell:   defaultRemoteExecutionShell,
		sudo:    sudo,
	}

	command, err := raw.ParseCommand()
	if err != nil {
		return err
	}

	err = cluster.RunCommand(
		command,
		serial,
	)
	if err != nil {
		return hierr.Errorf(
			err,
			`can't start tar extraction command: '%v'`,
			command,
		)
	}

	return nil
}

func archiveFilesToWriter(
	target io.WriteCloser,
	files []file,
	preserveUID, preserveGID bool,
) error {
	workDir, err := os.Getwd()
	if err != nil {
		return hierr.Errorf(
			err,
			`can't get current working directory`,
		)
	}

	stat := &struct {
		Phase   string
		Total   int
		Fails   int
		Success int
		Written bytesStringer
		Bytes   bytesStringer
	}{
		Phase: "upload",
		Total: len(files),
	}

	log.SetStatus(stat)

	for _, file := range files {
		stat.Bytes.Amount += file.size
	}

	archiveWriter := tar.NewWriter(target)
	stream := io.MultiWriter(archiveWriter, writer.CallbackWriter(
		func(data []byte) (int, error) {
			stat.Written.Amount += len(data)

			log.DrawStatus()

			return len(data), nil
		},
	))

	for fileIndex, file := range files {
		log.Infof(
			"%5d/%d sending file: '%s'",
			fileIndex+1,
			len(files),
			file.path,
		)

		err = writeFileToArchive(
			file.path,
			stream,
			archiveWriter,
			workDir,
			preserveUID,
			preserveGID,
		)
		if err != nil {
			return hierr.Errorf(
				err,
				`can't write file to archive: '%s'`,
				file.path,
			)
		}

		stat.Success++
	}

	log.Tracef("closing archive stream, %d files sent", len(files))

	err = archiveWriter.Close()
	if err != nil {
		return hierr.Errorf(
			err,
			`can't close tar stream`,
		)
	}

	err = target.Close()
	if err != nil {
		return hierr.Errorf(
			err,
			`can't close target stdin`,
		)
	}

	return nil
}

func writeFileToArchive(
	fileName string,
	stream io.Writer,
	archive *tar.Writer,
	workDir string,
	preserveUID, preserveGID bool,
) error {
	fileInfo, err := os.Stat(fileName)

	if err != nil {
		return hierr.Errorf(
			err,
			`can't stat file for archiving: '%s`, fileName,
		)
	}

	// avoid tar warnings about leading slash
	tarFileName := fileName
	if tarFileName[0] == '/' {
		tarFileName = tarFileName[1:]

		fileName, err = filepath.Rel(workDir, fileName)
		if err != nil {
			return hierr.Errorf(
				err,
				`can't make relative path from: '%s'`,
				fileName,
			)
		}
	}

	header := &tar.Header{
		Name: tarFileName,
		Mode: int64(fileInfo.Sys().(*syscall.Stat_t).Mode),
		Size: fileInfo.Size(),

		ModTime: fileInfo.ModTime(),
	}

	if preserveUID {
		header.Uid = int(fileInfo.Sys().(*syscall.Stat_t).Uid)
	}

	if preserveGID {
		header.Gid = int(fileInfo.Sys().(*syscall.Stat_t).Gid)
	}

	log.Tracef(
		hierr.Errorf(
			fmt.Sprintf(
				"size: %d bytes; mode: %o; uid/gid: %d/%d; modtime: %s",
				header.Size,
				header.Mode,
				header.Uid,
				header.Gid,
				header.ModTime,
			),
			`local file: %s; remote file: %s`,
			fileName,
			tarFileName,
		).Error(),
	)

	err = archive.WriteHeader(header)

	if err != nil {
		return hierr.Errorf(
			err,
			`can't write tar header for fileName: '%s'`, fileName,
		)
	}

	fileToArchive, err := os.Open(fileName)
	if err != nil {
		return hierr.Errorf(
			err,
			`can't open fileName for reading: '%s'`,
			fileName,
		)
	}

	_, err = io.Copy(stream, fileToArchive)
	if err != nil {
		return hierr.Errorf(
			err,
			`can't copy file to the archive: '%s'`,
			fileName,
		)
	}

	return nil
}

func getFilesList(relative bool, sources ...string) ([]file, error) {
	files := []file{}

	for _, source := range sources {
		err := filepath.Walk(
			source,
			func(path string, info os.FileInfo, err error) error {
				if err != nil {
					return err
				}

				if info.IsDir() {
					return nil
				}

				if !relative {
					path, err = filepath.Abs(path)
					if err != nil {
						return hierr.Errorf(
							err,
							`can't get absolute path for local file: '%s'`,
							path,
						)
					}
				}

				files = append(files, file{
					path: path,
					size: int(info.Size()),
				})

				return nil
			},
		)

		if err != nil {
			return nil, err
		}
	}

	return files, nil
}
