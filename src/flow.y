// %start Expr
// %token TEXT INT IDENT '>=' '<=' '>' '<' '<>' '!=' '(' ')' 'BOOL' 'LIKE' 'NLIKE' 'IN' 'NIN' 'INT_ARRAY' 'TEXT_ARRAY'
// %left '||'
// %right '&&'

// %%
// Expr -> Expr:
//     Factor { $1 }
//   | Exprs  { $1 }
//   ;


// Exprs -> Expr:
// ;
// %%

// // use lrpar::Span;
// use crate::*;