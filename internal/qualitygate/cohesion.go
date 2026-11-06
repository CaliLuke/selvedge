package main

import (
	"go/ast"
	"go/token"
)

// substantialComponents counts disconnected declaration-reference groups in a
// large file. Receiver methods share an anchor even when the type lives elsewhere.
// It is an AST signal, not a type-resolved claim about architectural concerns.
func substantialComponents(file *ast.File, fs *token.FileSet) int {
	parents := make([]int, len(file.Decls))
	names := map[string]int{}
	receivers := map[string]int{}
	for i, decl := range file.Decls {
		parents[i] = i
		switch d := decl.(type) {
		case *ast.FuncDecl:
			if d.Recv == nil {
				names[d.Name.Name] = i
			} else {
				receiver := receiverName(d.Recv.List[0].Type)
				if previous, ok := receivers[receiver]; ok {
					join(parents, i, previous)
				} else {
					receivers[receiver] = i
				}
			}
		case *ast.GenDecl:
			for _, spec := range d.Specs {
				switch s := spec.(type) {
				case *ast.TypeSpec:
					names[s.Name.Name] = i
				case *ast.ValueSpec:
					for _, name := range s.Names {
						names[name.Name] = i
					}
				}
			}
		}
	}
	for i, decl := range file.Decls {
		ast.Inspect(decl, func(n ast.Node) bool {
			if ident, ok := n.(*ast.Ident); ok {
				if other, exists := names[ident.Name]; exists {
					join(parents, i, other)
				}
			}
			return true
		})
	}
	lines, declarations := map[int]int{}, map[int]int{}
	for i, decl := range file.Decls {
		root := rootOf(parents, i)
		lines[root] += fs.Position(decl.End()).Line - fs.Position(decl.Pos()).Line + 1
		declarations[root]++
	}
	count := 0
	for root, size := range lines {
		if size >= 80 || (size >= 40 && declarations[root] >= 2) {
			count++
		}
	}
	return count
}

func rootOf(parents []int, n int) int {
	for parents[n] != n {
		n = parents[n]
	}
	return n
}

func join(parents []int, a, b int) { parents[rootOf(parents, a)] = rootOf(parents, b) }
