// Command qualitygate inventories owned Go source for structural and coverage gates.
package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

const buildTags = "integration,postgres,pglocal"

type packageInfo struct {
	Dir, ImportPath   string
	GoFiles, CgoFiles []string
	Error             *struct{ Err string }
}

type report struct {
	MetricKeys     []string                  `json:"metric_keys"`
	Version        int                       `json:"version"`
	Tags           string                    `json:"tags"`
	Packages       map[string][]string       `json:"packages"`
	SourceHashes   map[string]string         `json:"source_hashes"`
	GeneratedFiles []string                  `json:"generated_files"`
	CoverageFiles  []string                  `json:"coverage_files"`
	Metrics        map[string]map[string]int `json:"metrics"`
}

func main() {
	if err := run(os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(out io.Writer) error {
	cmd := exec.Command("go", "list", "-json", "-tags="+buildTags, "./...")
	cmd.Stderr = os.Stderr
	data, err := cmd.Output()
	if err != nil {
		return fmt.Errorf("package inventory: %w", err)
	}
	root, err := os.Getwd()
	if err != nil {
		return err
	}
	r, err := inventory(root, strings.NewReader(string(data)))
	if err != nil {
		return err
	}
	return json.NewEncoder(out).Encode(r)
}

func inventory(root string, input io.Reader) (*report, error) {
	r := &report{Version: 1, Tags: buildTags, Packages: map[string][]string{}, SourceHashes: map[string]string{}, Metrics: map[string]map[string]int{}}
	decoder := json.NewDecoder(input)
	for {
		var pkg packageInfo
		if err := decoder.Decode(&pkg); err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		if pkg.Error != nil {
			return nil, fmt.Errorf("package %s: %s", pkg.ImportPath, pkg.Error.Err)
		}
		if pkg.ImportPath == "" {
			return nil, fmt.Errorf("package is missing import path")
		}
		if _, exists := r.Packages[pkg.ImportPath]; exists {
			return nil, fmt.Errorf("duplicate package %s", pkg.ImportPath)
		}
		r.Packages[pkg.ImportPath] = []string{}
		for _, name := range append(pkg.GoFiles, pkg.CgoFiles...) {
			path := filepath.Join(pkg.Dir, name)
			rel, err := filepath.Rel(root, path)
			if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
				return nil, fmt.Errorf("source outside repository: %s", path)
			}
			if err := r.addFile(pkg.ImportPath, filepath.ToSlash(rel), path); err != nil {
				return nil, err
			}
		}
	}
	if len(r.Packages) == 0 {
		return nil, fmt.Errorf("empty package inventory")
	}
	return r, nil
}

func (r *report) metric(key string) map[string]int {
	if r.Metrics[key] == nil {
		r.Metrics[key] = map[string]int{}
		r.MetricKeys = append(r.MetricKeys, key)
	}
	return r.Metrics[key]
}

func (r *report) addFile(pkg, relative, path string) error {
	fs := token.NewFileSet()
	source, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	file, err := parser.ParseFile(fs, path, source, parser.ParseComments)
	if err != nil {
		return err
	}
	if ast.IsGenerated(file) {
		r.GeneratedFiles = append(r.GeneratedFiles, relative)
		return nil
	}
	r.Packages[pkg] = append(r.Packages[pkg], relative)
	hash := sha256.Sum256(source)
	r.SourceHashes[relative] = hex.EncodeToString(hash[:])
	lines := fs.Position(file.End()).Line
	r.metric("file:" + relative)["lines"] = lines
	r.metric("file:" + relative)["components"] = 0
	if lines > 600 {
		r.metric("file:" + relative)["components"] = substantialComponents(file, fs)
	}
	r.metric("package:" + pkg)["files"]++
	r.metric("package:" + pkg)["lines"] += lines
	coverable := false
	for _, decl := range file.Decls {
		switch d := decl.(type) {
		case *ast.FuncDecl:
			if d.Body != nil && len(d.Body.List) > 0 {
				coverable = true
			}
			if d.Recv == nil {
				continue
			}
			key := "receiver:" + pkg + "." + receiverName(d.Recv.List[0].Type)
			r.metric(key)["methods"]++
			r.metric(key)["lines"] += fs.Position(d.End()).Line - fs.Position(d.Pos()).Line + 1
		case *ast.GenDecl:
			for _, spec := range d.Specs {
				typeSpec, ok := spec.(*ast.TypeSpec)
				if !ok {
					continue
				}
				strct, ok := typeSpec.Type.(*ast.StructType)
				if !ok {
					continue
				}
				dependencies := map[string]bool{}
				ast.Inspect(strct.Fields, func(n ast.Node) bool {
					if sel, ok := n.(*ast.SelectorExpr); ok {
						if ident, ok := sel.X.(*ast.Ident); ok {
							dependencies[ident.Name] = true
						}
					}
					return true
				})
				r.metric("holder:" + pkg + "." + typeSpec.Name.Name)["dependencies"] = len(dependencies)
			}
		}
	}
	if coverable {
		r.CoverageFiles = append(r.CoverageFiles, relative)
	}
	return nil
}

func receiverName(expr ast.Expr) string {
	switch e := expr.(type) {
	case *ast.Ident:
		return e.Name
	case *ast.StarExpr:
		return receiverName(e.X)
	case *ast.IndexExpr:
		return receiverName(e.X)
	case *ast.IndexListExpr:
		return receiverName(e.X)
	default:
		return "<unknown>"
	}
}
